"""Print candle messages from Kafka for a human-readable live check."""
import json
import uuid

from confluent_kafka import Consumer

from config.settings import KAFKA_BOOTSTRAP_SERVERS, TOPIC_CANDLES
from src.common.kafka_utils import create_topic_if_not_exists


def main():
    """Subscribe with a fresh consumer group and print each candle as JSON."""
    create_topic_if_not_exists(TOPIC_CANDLES)
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": f"candles-console-{uuid.uuid4().hex}",
        "auto.offset.reset": "latest",
    })
    consumer.subscribe([TOPIC_CANDLES])
    print(
        f"Listening for candles on '{TOPIC_CANDLES}' "
        f"({KAFKA_BOOTSTRAP_SERVERS}). Press Ctrl+C to stop.",
        flush=True,
    )

    try:
        while True:
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                raise RuntimeError(f"Error consumiendo Kafka: {message.error()}")

            candle = json.loads(message.value().decode("utf-8"))
            print(json.dumps(candle, ensure_ascii=True), flush=True)
    except KeyboardInterrupt:
        print("Candle consumer stopped.", flush=True)
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
