"""
Clase base para todos los algoritmos de trading.
Cada algoritmo concreto solo hereda y sobrescribe `generate_signal`.
"""
from abc import ABC, abstractmethod
from collections import deque
from datetime import datetime
from typing import Optional

import uuid

from config.settings import KAFKA_BOOTSTRAP_SERVERS, TOPIC_VELAS, TOPIC_SIGNALS
from src.common.kafka_utils import (
    create_topic_if_not_exists,
    make_consumer,
    make_producer,
    serialize_json,
    deserialize_json,
)
from src.common.logger import get_logger
from src.runtime.state import ALGO_STATUS_FILE, install_graceful_shutdown, now_iso, write_json_atomic


class BaseAlgorithm(ABC):
    def __init__(self, id_algoritmo: str, buffer_size: int = 100):
        self.id_algoritmo = id_algoritmo
        self.buffer = deque(maxlen=buffer_size)
        self.last_signal: Optional[str] = None  # para no repetir BUY seguidos
        self.log = get_logger(id_algoritmo)
        # Clientes Kafka perezosos (los tests no los necesitan)
        self.consumer = None
        self.producer = None
        # Heartbeat para el dashboard: solo se activa en run() (los tests no escriben archivos)
        self.heartbeat_enabled = False
        self.velas_received = 0
        self.last_signal_at: Optional[str] = None
        self._last_inicio: Optional[str] = None

    @abstractmethod
    def generate_signal(self, velas: list) -> Optional[dict]:
        """
        Recibe la lista de velas actuales (incluyendo la más reciente al final).
        Retorna None o un dict con:
          - "signal": "BUY" | "SELL"
          - "precio_referencia": float
        """
        pass

    def warmup_needed(self) -> int:
        """Velas necesarias antes de la primera señal (las subclases lo sobrescriben)."""
        return 0

    def extra_status(self, velas: list) -> dict:
        """Datos extra del modelo para el dashboard (EMAs, RSI, ...). Opcional."""
        return {}

    def _write_heartbeat(self, vela: dict) -> None:
        try:
            velas = list(self.buffer)
            write_json_atomic(ALGO_STATUS_FILE, {
                "id_algoritmo": self.id_algoritmo,
                "updated_at": now_iso(),
                "symbol": vela.get("symbol"),
                "last_vela": {k: vela.get(k) for k in ("inicio", "open", "high", "low", "close", "num_ticks")},
                "velas_received": self.velas_received,
                "buffer_len": len(velas),
                "warmup_needed": self.warmup_needed(),
                "last_signal": self.last_signal,
                "last_signal_at": self.last_signal_at,
                "model": self.extra_status(velas),
            })
        except Exception as e:  # el heartbeat nunca debe tumbar al algoritmo
            self.log.error(f"Error escribiendo heartbeat: {e}")

    def _prewarm(self, n: int) -> int:
        """Rellena el buffer con las últimas `n` velas del tópico (sin emitir señales),
        así pausar/reiniciar el worker no obliga a esperar ~1 h de calentamiento."""
        from confluent_kafka import Consumer, TopicPartition
        consumer = Consumer({
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": f"{self.id_algoritmo}-prewarm-{uuid.uuid4().hex[:8]}",
            "enable.auto.commit": False,
        })
        got = 0
        try:
            tp = TopicPartition(TOPIC_VELAS, 0)
            low, high = consumer.get_watermark_offsets(tp, timeout=5)
            start = max(low, high - n)
            if high <= start:
                return 0
            consumer.assign([TopicPartition(TOPIC_VELAS, 0, start)])
            while True:
                msg = consumer.poll(2.0)
                if msg is None:
                    break
                if msg.error():
                    continue
                vela = deserialize_json(msg.value())
                self.buffer.append(vela)
                self._last_inicio = str(vela.get("inicio"))
                got += 1
                if msg.offset() >= high - 1:
                    break
        finally:
            consumer.close()
        return got

    def _publish_signal(self, vela: dict, result: dict) -> None:
        """Publica la señal al tópico `signals`. Idempotente por `last_signal`."""
        if result["signal"] == self.last_signal:
            return  # evitar repeticiones

        signal_msg = {
            "id_algoritmo": self.id_algoritmo,
            "symbol": vela["symbol"],
            "signal": result["signal"],
            "precio_referencia": float(result["precio_referencia"]),
            "timestamp": datetime.utcnow().isoformat() + "Z",
        }
        try:
            self.producer.produce(
                TOPIC_SIGNALS,
                key=self.id_algoritmo.encode("utf-8"),
                value=serialize_json(signal_msg),
            )
            self.producer.poll(0)
            self.log.info(f"🔔 Señal emitida: {signal_msg}")
            self.last_signal = result["signal"]
            self.last_signal_at = signal_msg["timestamp"]
        except Exception as e:
            self.log.error(f"Error publicando señal: {e}")

    def process_vela(self, vela: dict) -> Optional[dict]:
        """
        Método principal testeable: agrega la vela al buffer, calcula señal
        y, si hay productor activo, publica en Kafka.
        Retorna la señal (o None) para facilitar los tests.
        """
        self.buffer.append(vela)
        self.velas_received += 1
        try:
            result = self.generate_signal(list(self.buffer))
        except Exception as e:
            self.log.error(f"Error en generate_signal: {e}")
            return None

        if result is None:
            if self.heartbeat_enabled:
                self._write_heartbeat(vela)
            return None

        if self.producer is not None:
            self._publish_signal(vela, result)
        else:
            # Modo test: solo actualizamos last_signal sin publicar
            self.last_signal = result["signal"]

        if self.heartbeat_enabled:
            self._write_heartbeat(vela)
        return result

    def run(self) -> None:
        """Bucle principal: consume velas de Kafka y publica señales."""
        # Crear el tópico signals ANTES de producir (lección de Fase 2)
        create_topic_if_not_exists(TOPIC_SIGNALS)

        install_graceful_shutdown()
        self.heartbeat_enabled = True
        try:
            n = self._prewarm(self.warmup_needed() + 10)
            self.log.info(f"Buffer precalentado con {n} velas históricas del tópico '{TOPIC_VELAS}'.")
        except Exception as e:
            self.log.warning(f"No se pudo precalentar el buffer ({e}); se calentará con velas en vivo.")

        self.consumer = make_consumer(TOPIC_VELAS, group_id=f"{self.id_algoritmo}-group")
        self.producer = make_producer()
        self.log.info(
            f"Algoritmo iniciado: '{self.id_algoritmo}'. "
            f"Escuchando '{TOPIC_VELAS}', publicando a '{TOPIC_SIGNALS}'."
        )
        self.log.info(
            "⚠ Se requieren ~22 velas iniciales antes de la primera señal "
            "(periodo de calentamiento de la EMA lenta)."
        )

        try:
            while True:
                msg = self.consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    self.log.error(f"Kafka error: {msg.error()}")
                    continue
                vela = deserialize_json(msg.value())
                if self._last_inicio is not None and str(vela.get("inicio")) <= self._last_inicio:
                    continue  # ya procesada en el precalentamiento
                self._last_inicio = None
                self.process_vela(vela)
        except KeyboardInterrupt:
            self.log.info("Detenido por el usuario (Ctrl+C).")
        finally:
            if self.consumer is not None:
                self.consumer.close()
            if self.producer is not None:
                self.producer.flush(timeout=5)
            self.log.info(f"Algoritmo '{self.id_algoritmo}' detenido.")