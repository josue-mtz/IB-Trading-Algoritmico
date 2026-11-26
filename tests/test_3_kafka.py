"""Kafka integration test for publishing signals from synthetic candle data."""
import json
import time
import uuid

from confluent_kafka import Consumer, TopicPartition

from config.settings import KAFKA_BOOTSTRAP_SERVERS, TOPIC_SIGNALS
from src.algorithms.ema_cross import EMACross
from src.common.kafka_utils import create_topic_if_not_exists, make_producer


def test_synthetic_buy_reaches_kafka():
    test_id = f"ema_cross_test_{uuid.uuid4().hex[:8]}"
    create_topic_if_not_exists(TOPIC_SIGNALS)

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": f"{test_id}-group",
        "auto.offset.reset": "latest",
    })
    _, latest_offset = consumer.get_watermark_offsets(
        TopicPartition(TOPIC_SIGNALS, 0),
        timeout=5,
    )
    consumer.assign([TopicPartition(TOPIC_SIGNALS, 0, latest_offset)])

    algorithm = EMACross(fast=9, slow=21)
    algorithm.id_algoritmo = test_id
    algorithm.producer = make_producer()

    try:
        assignment_deadline = time.monotonic() + 10
        while not consumer.assignment():
            consumer.poll(timeout=0.1)
            if time.monotonic() >= assignment_deadline:
                raise AssertionError("The signal consumer did not receive a partition.")

        prices = [1.0800] * 25
        prices += [1.0795, 1.0790, 1.0785]
        prices += [1.0820, 1.0850, 1.0880]

        signals = []
        for index, price in enumerate(prices):
            candle = {
                "symbol": "EURUSD",
                "inicio": f"2026-01-01T12:{index:02d}:00.000Z",
                "open": price,
                "high": price,
                "low": price,
                "close": price,
                "num_ticks": 1,
            }
            result = algorithm.process_vela(candle)
            if result is not None:
                signals.append(result["signal"])

        remaining = algorithm.producer.flush(timeout=10)
        assert remaining == 0, f"{remaining} signals remain undelivered."

        messages = []
        deadline = time.monotonic() + 10
        while len(messages) < len(signals) and time.monotonic() < deadline:
            message = consumer.poll(timeout=0.5)
            if message is None:
                continue
            if message.error():
                raise AssertionError(f"Error reading a Kafka signal: {message.error()}")

            payload = json.loads(message.value().decode("utf-8"))
            if payload.get("id_algoritmo") == test_id:
                messages.append(payload)

        assert signals == ["SELL", "BUY"], f"Unexpected synthetic sequence: {signals}"
        assert len(messages) == 2, f"Received {len(messages)} test strategy signals."
        assert [message["signal"] for message in messages] == signals
        assert sum(message["signal"] == "BUY" for message in messages) == 1

        expected_fields = {
            "id_algoritmo",
            "symbol",
            "signal",
            "precio_referencia",
            "timestamp",
        }
        for message in messages:
            assert set(message) == expected_fields, f"Unexpected fields: {set(message)}"
            assert message["symbol"] == "EURUSD"
            assert isinstance(message["precio_referencia"], (int, float))
            assert message["timestamp"].endswith("Z")
            print(f"JSON Kafka: {json.dumps(message, ensure_ascii=False)}")
    finally:
        consumer.close()
        algorithm.producer.flush(timeout=5)


if __name__ == "__main__":
    test_synthetic_buy_reaches_kafka()
