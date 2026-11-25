"""
Clase base para todos los algoritmos de trading.
Cada algoritmo concreto solo hereda y sobrescribe `generate_signal`.
"""
from abc import ABC, abstractmethod
from collections import deque
from datetime import datetime
from typing import Optional

from config.settings import TOPIC_VELAS, TOPIC_SIGNALS
from src.common.kafka_utils import (
    create_topic_if_not_exists,
    make_consumer,
    make_producer,
    serialize_json,
    deserialize_json,
)
from src.common.logger import get_logger


class BaseAlgorithm(ABC):
    def __init__(self, id_algoritmo: str, buffer_size: int = 100):
        self.id_algoritmo = id_algoritmo
        self.buffer = deque(maxlen=buffer_size)
        self.last_signal: Optional[str] = None  # para no repetir BUY seguidos
        self.log = get_logger(id_algoritmo)
        # Clientes Kafka perezosos (los tests no los necesitan)
        self.consumer = None
        self.producer = None

    @abstractmethod
    def generate_signal(self, velas: list) -> Optional[dict]:
        """
        Recibe la lista de velas actuales (incluyendo la más reciente al final).
        Retorna None o un dict con:
          - "signal": "BUY" | "SELL"
          - "precio_referencia": float
        """
        pass

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
        except Exception as e:
            self.log.error(f"Error publicando señal: {e}")

    def process_vela(self, vela: dict) -> Optional[dict]:
        """
        Método principal testeable: agrega la vela al buffer, calcula señal
        y, si hay productor activo, publica en Kafka.
        Retorna la señal (o None) para facilitar los tests.
        """
        self.buffer.append(vela)
        try:
            result = self.generate_signal(list(self.buffer))
        except Exception as e:
            self.log.error(f"Error en generate_signal: {e}")
            return None

        if result is None:
            return None

        if self.producer is not None:
            self._publish_signal(vela, result)
        else:
            # Modo test: solo actualizamos last_signal sin publicar
            self.last_signal = result["signal"]

        return result

    def run(self) -> None:
        """Bucle principal: consume velas de Kafka y publica señales."""
        # Crear el tópico signals ANTES de producir (lección de Fase 2)
        create_topic_if_not_exists(TOPIC_SIGNALS)

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
                self.process_vela(vela)
        except KeyboardInterrupt:
            self.log.info("Detenido por el usuario (Ctrl+C).")
        finally:
            if self.consumer is not None:
                self.consumer.close()
            if self.producer is not None:
                self.producer.flush(timeout=5)
            self.log.info(f"Algoritmo '{self.id_algoritmo}' detenido.")