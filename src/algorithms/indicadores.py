"""Compatibility imports for the former Spanish technical-indicator module."""
from src.algorithms.indicators import (
    detect_cross,
    ema_last,
    ema_series,
    rsi_series,
)

__all__ = ["detect_cross", "ema_last", "ema_series", "rsi_series"]
