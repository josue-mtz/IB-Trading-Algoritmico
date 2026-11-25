import json
import time
import uuid
from confluent_kafka import Producer
from config.settings import KAFKA_BOOTSTRAP_SERVERS, TOPIC_TICKS
from src.common.kafka_utils import create_topic_if_not_exists, make_producer, make_consumer, serialize_json

TEST_TOPIC = "test-fase1-roundtrip"

def test_kafka_connection():
    """Verifica que Kafka esté corriendo y responde."""
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})
    # Si no lanza excepción, está conectado
    producer.list_topics(timeout=5)
    print("Kafka responde en", KAFKA_BOOTSTRAP_SERVERS)

def test_create_topic():
    """Verifica que se pueda crear un tópico."""
    create_topic_if_not_exists(TOPIC_TICKS)
    print(f"Tópico '{TOPIC_TICKS}' disponible.")

def test_producer_consumer_roundtrip():
    """Publica y consume un mensaje en un tópico aislado de pruebas."""
    create_topic_if_not_exists(TEST_TOPIC)
    test_msg = {"symbol": "EURUSD", "bid": 1.08500, "ask": 1.08502, "timestamp": "2025-01-01T00:00:00Z"}
    c = make_consumer(TEST_TOPIC, group_id=f"test-group-fase1-{uuid.uuid4().hex}")
    try:
        assignment_deadline = time.monotonic() + 10
        while not c.assignment():
            c.poll(timeout=0.1)
            if time.monotonic() >= assignment_deadline:
                raise AssertionError("El consumidor no recibió asignación de partición.")

        delivery_errors = []
        p = make_producer()
        p.produce(
            TEST_TOPIC,
            value=serialize_json(test_msg),
            on_delivery=lambda error, _message: delivery_errors.append(error),
        )
        remaining = p.flush(timeout=10)
        assert remaining == 0, f"Quedaron {remaining} mensajes sin entregar."
        assert delivery_errors and delivery_errors[0] is None, "Kafka rechazó el mensaje."
        print("Mensaje de prueba publicado.")

        msg = c.poll(timeout=10.0)
        assert msg is not None, "No se recibió el mensaje de vuelta."
        assert msg.error() is None, f"Error al consumir el mensaje: {msg.error()}"
        received = json.loads(msg.value().decode("utf-8"))
        assert received["symbol"] == "EURUSD"
        assert "bid" in received and "ask" in received and "timestamp" in received
        print("Mensaje recibido y validado:", received)
    finally:
        c.close()

if __name__ == "__main__":
    print("Ejecutando tests de Fase 1 ")
    test_kafka_connection()
    test_create_topic()
    test_producer_consumer_roundtrip()
    print("Todos los tests de Fase 1 pasaron. ")