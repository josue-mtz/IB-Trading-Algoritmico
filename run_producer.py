"""Start the Interactive Brokers-to-Kafka market-data producer."""
from src.producer.ib_tick_producer import IBTickProducer


if __name__ == "__main__":
    producer = IBTickProducer()
    producer.run()