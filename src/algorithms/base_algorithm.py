"""Clase base para algoritmos de trading en flujo continuo.

Las estrategias concretas implementan :meth:`generate_signal`; esta clase
gestiona la entrada y salida de Kafka, el calentamiento de indicadores, la
supresión de señales duplicadas y el estado periódico usado por el panel.
"""
from abc import ABC, abstractmethod
from collections import deque
from datetime import datetime
from typing import Optional

import uuid

from config.settings import KAFKA_BOOTSTRAP_SERVERS, TOPIC_CANDLES, TOPIC_SIGNALS
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
    """Consume mensajes de velas, gestiona la estrategia y publica señales de trading."""

    def __init__(self, id_algoritmo: str, buffer_size: int = 100):
        self.id_algoritmo = id_algoritmo
        self.buffer = deque(maxlen=buffer_size)
        self.last_signal: Optional[str] = None
        self.log = get_logger(id_algoritmo)
        # Las conexiones se crean en run() para permitir pruebas unitarias sin servicios.
        self.consumer = None
        self.producer = None
        # El estado periódico solo se activa en run(); las pruebas no escriben archivos de ejecución.
        self.heartbeat_enabled = False
        self.velas_received = 0
        self.last_signal_at: Optional[str] = None
        self._last_candle_time: Optional[str] = None

    @abstractmethod
    def generate_signal(self, candles: list) -> Optional[dict]:
        """Devuelve None o un mensaje de señal para la vela más reciente.

        Los diccionarios conservan los campos Kafka ``signal`` y
        ``precio_referencia`` para mantener compatibilidad con consumidores existentes.
        """
        raise NotImplementedError

    def warmup_needed(self) -> int:
        """Devuelve cuántas velas necesita la estrategia antes de emitir señales."""
        return 0

    def extra_status(self, candles: list) -> dict:
        """Devuelve detalles opcionales de la estrategia para el panel."""
        return {}

    def _write_heartbeat(self, candle: dict) -> None:
        try:
            candles = list(self.buffer)
            write_json_atomic(ALGO_STATUS_FILE, {
                "id_algoritmo": self.id_algoritmo,
                "updated_at": now_iso(),
                "symbol": candle.get("symbol"),
                # Se conservan las claves anteriores para los clientes del panel existentes.
                "last_vela": {
                    key: candle.get(key)
                    for key in ("inicio", "open", "high", "low", "close", "num_ticks")
                },
                "velas_received": self.velas_received,
                "buffer_len": len(candles),
                "warmup_needed": self.warmup_needed(),
                "last_signal": self.last_signal,
                "last_signal_at": self.last_signal_at,
                "model": self.extra_status(candles),
            })
        except Exception as e:  # Un fallo al guardar el estado no debe detener el procesamiento.
            self.log.error("No se pudo escribir el estado del algoritmo para el dashboard: %s", e)

    def _prewarm(self, n: int) -> int:
        """Carga mensajes recientes de Kafka sin emitir señales.

        Así, un proceso reiniciado puede continuar tras un calentamiento breve,
        sin esperar de nuevo todo el historial que requieren los indicadores.
        """
    from confluent_kafka import Consumer, TopicPartition
        consumer = Consumer({
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": f"{self.id_algoritmo}-prewarm-{uuid.uuid4().hex[:8]}",
            "enable.auto.commit": False,
        })
        got = 0
        try:
            tp = TopicPartition(TOPIC_CANDLES, 0)
            low, high = consumer.get_watermark_offsets(tp, timeout=5)
            start = max(low, high - n)
            if high <= start:
                return 0
            consumer.assign([TopicPartition(TOPIC_CANDLES, 0, start)])
            while True:
                msg = consumer.poll(2.0)
                if msg is None:
                    break
                if msg.error():
                    continue
                candle = deserialize_json(msg.value())
                self.buffer.append(candle)
                self._last_candle_time = str(candle.get("inicio"))
                got += 1
                if msg.offset() >= high - 1:
                    break
        finally:
            consumer.close()
        return got

    def _publish_signal(self, candle: dict, result: dict) -> None:
        """Publica una señal, salvo que repita la dirección anterior."""
        if result["signal"] == self.last_signal:
            return

        signal_msg = {
            "id_algoritmo": self.id_algoritmo,
            "symbol": candle["symbol"],
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
            self.log.info("Señal publicada: %s", signal_msg)
            self.last_signal = result["signal"]
            self.last_signal_at = signal_msg["timestamp"]
        except Exception as e:
            self.log.error("No se pudo publicar la señal: %s", e)

    def process_candle(self, candle: dict) -> Optional[dict]:
        """Procesa una vela y publica la señal que se genere.

        El diccionario devuelto es útil en pruebas unitarias. Kafka solo se usa
        si ya se creó una conexión con el productor.
        """
        self.buffer.append(candle)
        self.velas_received += 1
        try:
            result = self.generate_signal(list(self.buffer))
        except Exception as e:
            self.log.exception("La estrategia falló al generar una señal: %s", e)
            return None

        if result is None:
            if self.heartbeat_enabled:
                self._write_heartbeat(candle)
            return None

        if self.producer is not None:
            self._publish_signal(candle, result)
        else:
            self.last_signal = result["signal"]

        if self.heartbeat_enabled:
            self._write_heartbeat(candle)
        return result

    def process_vela(self, vela: dict) -> Optional[dict]:
        """Alias de compatibilidad para código que aún usa el nombre anterior."""
        return self.process_candle(vela)

    def run(self) -> None:
        """Ejecuta el ciclo consumidor de Kafka hasta que se interrumpa."""
        create_topic_if_not_exists(TOPIC_SIGNALS)

        install_graceful_shutdown()
        self.heartbeat_enabled = True
        try:
            n = self._prewarm(self.warmup_needed() + 10)
            self.log.info("Se precargaron %s velas del tópico '%s'.", n, TOPIC_CANDLES)
        except Exception as e:
            self.log.warning(
                "No se pudo precargar el historial de velas (%s); se esperará a las velas en vivo.",
                e,
            )

        self.consumer = make_consumer(
            TOPIC_CANDLES,
            group_id=f"{self.id_algoritmo}-group",
        )
        self.producer = make_producer()
        self.log.info(
            "Algoritmo '%s' iniciado. Lee velas de '%s' y publica señales en '%s'.",
            self.id_algoritmo,
            TOPIC_CANDLES,
            TOPIC_SIGNALS,
        )
        self.log.info(
            "La estrategia necesita %s velas de calentamiento antes de dar su primera señal.",
            self.warmup_needed(),
        )

        try:
            while True:
                msg = self.consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    self.log.error("Error de Kafka: %s", msg.error())
                    continue
                candle = deserialize_json(msg.value())
                candle_time = str(candle.get("inicio"))
                if (
                    self._last_candle_time is not None
                    and candle_time <= self._last_candle_time
                ):
                    continue
                self._last_candle_time = candle_time
                self.process_candle(candle)
        except KeyboardInterrupt:
            self.log.info("Detenido por el usuario (Ctrl+C).")
        finally:
            if self.consumer is not None:
                self.consumer.close()
            if self.producer is not None:
                self.producer.flush(timeout=5)
            self.log.info("Algoritmo '%s' detenido.", self.id_algoritmo)