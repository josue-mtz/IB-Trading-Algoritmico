"""Start the Spark job that converts Kafka ticks into time-based candles."""
import os
import sys

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

from src.spark.ticks_to_candles import run

if __name__ == "__main__":
    run()