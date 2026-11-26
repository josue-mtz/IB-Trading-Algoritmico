"""Funciones auxiliares para crear tópicos y productores o consumidores JSON de Kafka."""
import json
from confluent_kafka import Producer, Consumer, KafkaError
from confluent_kafka.admin import AdminClient, NewTopic
from config.settings import KAFKA_BOOTSTRAP_SERVERS
from src.common.logger import get_logger

log = get_logger("kafka_utils")


def create_topic_if_not_exists(topic_name: str, num_partitions: int = 1, replication_factor: int = 1):
    """Crea un tópico de Kafka si no existe; no modifica los tópicos existentes."""
    admin_client = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})
    metadata = admin_client.list_topics(timeout=5)
    if topic_name in metadata.topics:
        log.info("Kafka topic '%s' already exists.", topic_name)
        return

    new_topic = NewTopic(
        topic_name,
        num_partitions=num_partitions,
        replication_factor=replication_factor,
    )
    fs = admin_client.create_topics([new_topic])
    for topic, f in fs.items():
        try:
            f.result()
            log.info("Kafka topic '%s' created.", topic)
        except Exception as e:
            log.error("Could not create Kafka topic '%s': %s", topic, e)


def make_producer() -> Producer:
    """Crea y devuelve un productor de Kafka para el broker configurado."""
    return Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})


def make_consumer(topic: str, group_id: str) -> Consumer:
    """Crea un consumidor de Kafka suscrito a un tópico."""
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": group_id,
        "auto.offset.reset": "latest",
    })
    consumer.subscribe([topic])
    return consumer


def serialize_json(data: dict) -> bytes:
    """Codifica un diccionario como bytes JSON UTF-8 para Kafka."""
    return json.dumps(data, default=str).encode("utf-8")


def deserialize_json(data: bytes) -> dict:
    """Decodifica un mensaje Kafka que contiene un objeto JSON UTF-8."""
    return json.loads(data.decode("utf-8"))