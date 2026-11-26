"""Inicia el productor de datos de mercado desde Interactive Brokers hacia Kafka."""
from src.producer.ib_tick_producer import IBTickProducer


if __name__ == "__main__":
    producer = IBTickProducer()
    producer.run()