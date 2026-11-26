"""Inicia el proceso Spark que convierte los ticks de Kafka en velas por intervalo."""
import os
import sys

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

from src.spark.ticks_to_candles import run

if __name__ == "__main__":
    run()