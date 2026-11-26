"""Start the paper-trading order executor that consumes Kafka signals."""
from src.executor.signal_executor import SignalExecutor


if __name__ == "__main__":
    executor = SignalExecutor()
    executor.run()