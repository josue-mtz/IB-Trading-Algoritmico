"""Inicia el ejecutor de órdenes de paper trading que consume señales de Kafka."""
from src.executor.signal_executor import SignalExecutor


if __name__ == "__main__":
    executor = SignalExecutor()
    executor.run()