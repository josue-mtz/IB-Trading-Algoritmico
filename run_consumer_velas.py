import json
import uuid

from confluent_kafka import Consumer

from config.settings import KAFKA_BOOTSTRAP_SERVERS, TOPIC_VELAS
from src.common.kafka_utils import create_topic_if_not_exists


def main():
    create_topic_if_not_exists(TOPIC_VELAS)
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": f"velas-console-{uuid.uuid4().hex}",
        "auto.offset.reset": "latest",
    })
    consumer.subscribe([TOPIC_VELAS])
    print(f"Escuchando velas en '{TOPIC_VELAS}' ({KAFKA_BOOTSTRAP_SERVERS}). Ctrl+C para salir.", flush=True)

    try:
        while True:
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                raise RuntimeError(f"Error consumiendo Kafka: {message.error()}")

            vela = json.loads(message.value().decode("utf-8"))
            print(json.dumps(vela, ensure_ascii=False), flush=True)
    except KeyboardInterrupt:
        print("Consumidor detenido.", flush=True)
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
