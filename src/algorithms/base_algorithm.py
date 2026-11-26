"""Base class for streaming trading algorithms.

Concrete strategies implement :meth:`generate_signal`; this class manages
Kafka input/output, indicator warmup, duplicate-signal suppression, and the
status heartbeat used by the dashboard.
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
    """Consume candle messages, manage strategy state, and publish trade signals."""

    def __init__(self, id_algoritmo: str, buffer_size: int = 100):
        self.id_algoritmo = id_algoritmo
        self.buffer = deque(maxlen=buffer_size)
        self.last_signal: Optional[str] = None
        self.log = get_logger(id_algoritmo)
        # Connections are created in run() so unit tests can use the strategy offline.
        self.consumer = None
        self.producer = None
        # The heartbeat is enabled only by run(); unit tests do not write runtime files.
        self.heartbeat_enabled = False
        self.velas_received = 0
        self.last_signal_at: Optional[str] = None
        self._last_candle_time: Optional[str] = None

    @abstractmethod
    def generate_signal(self, candles: list) -> Optional[dict]:
        """Return no signal or a signal message for the newest candle.

        Signal dictionaries retain the established Kafka fields ``signal`` and
        ``precio_referencia`` for compatibility with existing consumers.
        """
        raise NotImplementedError

    def warmup_needed(self) -> int:
        """Return how many candles the strategy needs before it can emit signals."""
        return 0

    def extra_status(self, candles: list) -> dict:
        """Return optional strategy details for the dashboard."""
        return {}

    def _write_heartbeat(self, candle: dict) -> None:
        try:
            candles = list(self.buffer)
            write_json_atomic(ALGO_STATUS_FILE, {
                "id_algoritmo": self.id_algoritmo,
                "updated_at": now_iso(),
                "symbol": candle.get("symbol"),
                # Legacy status keys are retained for existing dashboard clients.
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
        except Exception as e:  # A status-file failure must not stop signal processing.
            self.log.error("Could not write algorithm status heartbeat: %s", e)

    def _prewarm(self, n: int) -> int:
        """Load recent Kafka history without emitting signals.

        This lets a restarted worker resume after a short warm-up instead of
        waiting for the full indicator history to arrive again.
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
        """Publish a signal unless it repeats the previous direction."""
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
            self.log.info("Signal published: %s", signal_msg)
            self.last_signal = result["signal"]
            self.last_signal_at = signal_msg["timestamp"]
        except Exception as e:
            self.log.error("Could not publish signal: %s", e)

    def process_candle(self, candle: dict) -> Optional[dict]:
        """Process one candle and publish any resulting signal.

        The returned dictionary is useful in unit tests; Kafka is only used
        when a producer connection has been created.
        """
        self.buffer.append(candle)
        self.velas_received += 1
        try:
            result = self.generate_signal(list(self.buffer))
        except Exception as e:
            self.log.exception("Strategy failed while generating a signal: %s", e)
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
        """Compatibility alias for callers that still use the former method name."""
        return self.process_candle(vela)

    def run(self) -> None:
        """Run the Kafka consumer loop until interrupted."""
        create_topic_if_not_exists(TOPIC_SIGNALS)

        install_graceful_shutdown()
        self.heartbeat_enabled = True
        try:
            n = self._prewarm(self.warmup_needed() + 10)
            self.log.info("Preloaded %s candles from topic '%s'.", n, TOPIC_CANDLES)
        except Exception as e:
            self.log.warning(
                "Could not preload candle history (%s); waiting for live candles.",
                e,
            )

        self.consumer = make_consumer(
            TOPIC_CANDLES,
            group_id=f"{self.id_algoritmo}-group",
        )
        self.producer = make_producer()
        self.log.info(
            "Algorithm '%s' started. Consuming '%s' and publishing to '%s'.",
            self.id_algoritmo,
            TOPIC_CANDLES,
            TOPIC_SIGNALS,
        )
        self.log.info(
            "The strategy requires %s warm-up candles before its first signal.",
            self.warmup_needed(),
        )

        try:
            while True:
                msg = self.consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    self.log.error("Kafka error: %s", msg.error())
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
            self.log.info("Stopped by user (Ctrl+C).")
        finally:
            if self.consumer is not None:
                self.consumer.close()
            if self.producer is not None:
                self.producer.flush(timeout=5)
            self.log.info("Algorithm '%s' stopped.", self.id_algoritmo)