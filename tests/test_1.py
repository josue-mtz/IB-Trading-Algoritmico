"""Kafka integration checks for connectivity, topic creation, and message delivery."""
import json
import time
import uuid
from confluent_kafka import Consumer, Producer, TopicPartition
from config.settings import KAFKA_BOOTSTRAP_SERVERS, TOPIC_TICKS
from src.common.kafka_utils import create_topic_if_not_exists, make_producer, serialize_json

TEST_TOPIC = "test-fase1-roundtrip"

def test_kafka_connection():
    """Verify that Kafka is running and accepting connections."""
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})
    # A successful metadata request confirms the broker connection.
    producer.list_topics(timeout=5)
    print("Kafka is reachable at", KAFKA_BOOTSTRAP_SERVERS)

def test_create_topic():
    """Verify that a Kafka topic can be created."""
    create_topic_if_not_exists(TOPIC_TICKS)
    print(f"Topic '{TOPIC_TICKS}' is available.")

def test_producer_consumer_roundtrip():
    """Publish and consume a message in an isolated test topic."""
    create_topic_if_not_exists(TEST_TOPIC)
    test_msg = {"symbol": "EURUSD", "bid": 1.08500, "ask": 1.08502, "timestamp": "2025-01-01T00:00:00Z"}
    c = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": f"test-roundtrip-{uuid.uuid4().hex}",
        "enable.auto.commit": False,
    })
    try:
        _, latest_offset = c.get_watermark_offsets(
            TopicPartition(TEST_TOPIC, 0),
            timeout=5,
        )
        c.assign([TopicPartition(TEST_TOPIC, 0, latest_offset)])

        delivery_errors = []
        p = make_producer()
        p.produce(
            TEST_TOPIC,
            value=serialize_json(test_msg),
            on_delivery=lambda error, _message: delivery_errors.append(error),
        )
        remaining = p.flush(timeout=10)
        assert remaining == 0, f"{remaining} messages remain undelivered."
        assert delivery_errors and delivery_errors[0] is None, "Kafka rejected the message."
        print("Test message published.")

        msg = None
        deadline = time.monotonic() + 10
        while msg is None and time.monotonic() < deadline:
            msg = c.poll(timeout=1.0)
        assert msg is not None, "The message was not received."
        assert msg.error() is None, f"Error while consuming the message: {msg.error()}"
        received = json.loads(msg.value().decode("utf-8"))
        assert received["symbol"] == "EURUSD"
        assert "bid" in received and "ask" in received and "timestamp" in received
        print("Message received and validated:", received)
    finally:
        c.close()

if __name__ == "__main__":
    print("Running Phase 1 tests")
    test_kafka_connection()
    test_create_topic()
    test_producer_consumer_roundtrip()
    print("All Phase 1 tests passed.")