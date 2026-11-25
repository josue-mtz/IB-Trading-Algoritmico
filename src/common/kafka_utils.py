import json
from confluent_kafka import Producer, Consumer, KafkaError
from confluent_kafka.admin import AdminClient, NewTopic
from config.settings import KAFKA_BOOTSTRAP_SERVERS
from src.common.logger import get_logger

log = get_logger("kafka_utils")

def create_topic_if_not_exists(topic_name: str, num_partitions: int = 1, replication_factor: int = 1):
    """Crea un tópico si no existe. No falla si ya existe."""
    admin_client = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})
    metadata = admin_client.list_topics(timeout=5)
    if topic_name in metadata.topics:
        log.info(f"El tópico '{topic_name}' ya existe.")
        return

    new_topic = NewTopic(topic_name, num_partitions=num_partitions, replication_factor=replication_factor)
    fs = admin_client.create_topics([new_topic])
    for topic, f in fs.items():
        try:
            f.result()
            log.info(f"Tópico '{topic}' creado exitosamente.")
        except Exception as e:
            log.error(f"Error creando tópico '{topic}': {e}")

def make_producer() -> Producer:
    """Crea y devuelve un productor de Kafka."""
    return Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})

def make_consumer(topic: str, group_id: str) -> Consumer:
    """Crea y devuelve un consumidor de Kafka suscrito a un tópico."""
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": group_id,
        "auto.offset.reset": "latest",
    })
    consumer.subscribe([topic])
    return consumer

def serialize_json(data: dict) -> bytes:
    return json.dumps(data, default=str).encode("utf-8")

def deserialize_json(data: bytes) -> dict:
    return json.loads(data.decode("utf-8"))