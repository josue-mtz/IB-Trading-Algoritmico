"""Compatibility imports for the former Spanish Spark module path."""
from src.spark.ticks_to_candles import (
    TICK_SCHEMA,
    get_spark_session,
    run,
    transform,
)

__all__ = ["TICK_SCHEMA", "get_spark_session", "run", "transform"]
