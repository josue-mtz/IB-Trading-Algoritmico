"""Pure technical-indicator calculations shared by live trading and backtests."""
from typing import List, Optional


def ema_series(values: List[float], period: int) -> List[Optional[float]]:
    """Return an exponential moving average series aligned with ``values``.

    The first ``period - 1`` entries are ``None``. The first calculated value
    uses a simple moving average; later values use the standard EMA formula.
    """
    if period < 1:
        raise ValueError("period must be at least 1")
    if len(values) < period:
        return [None] * len(values)

    result: List[Optional[float]] = [None] * (period - 1)
    result.append(sum(values[:period]) / period)
    alpha = 2.0 / (period + 1)
    for value in values[period:]:
        previous = result[-1]
        result.append(alpha * value + (1 - alpha) * previous)
    return result


def ema_last(values: List[float], period: int) -> Optional[float]:
    """Return the last EMA value, or ``None`` when there is not enough data."""
    series = ema_series(values, period)
    return series[-1] if series else None


def rsi_series(values: List[float], period: int = 14) -> List[Optional[float]]:
    """Return the Wilder relative-strength index aligned with ``values``.

    The first ``period`` entries are ``None`` because each RSI value requires
    ``period`` price changes. A flat window produces an RSI of 50.
    """
    if period < 1:
        raise ValueError("period must be at least 1")
    n = len(values)
    if n < period + 1:
        return [None] * n

    result: List[Optional[float]] = [None] * n
    gains = [0.0] * n
    losses = [0.0] * n
    for index in range(1, n):
        change = values[index] - values[index - 1]
        if change > 0:
            gains[index] = change
        else:
            losses[index] = -change

    def calculate_rsi(average_gain: float, average_loss: float) -> float:
        if average_loss == 0:
            return 100.0 if average_gain > 0 else 50.0
        relative_strength = average_gain / average_loss
        return 100.0 - 100.0 / (1.0 + relative_strength)

    average_gain = sum(gains[1: period + 1]) / period
    average_loss = sum(losses[1: period + 1]) / period
    result[period] = calculate_rsi(average_gain, average_loss)

    for index in range(period + 1, n):
        average_gain = (average_gain * (period - 1) + gains[index]) / period
        average_loss = (average_loss * (period - 1) + losses[index]) / period
        result[index] = calculate_rsi(average_gain, average_loss)

    return result


def detect_cross(
    previous_fast: Optional[float],
    previous_slow: Optional[float],
    current_fast: Optional[float],
    current_slow: Optional[float],
) -> Optional[str]:
    """Return ``BUY`` or ``SELL`` when two moving averages cross."""
    if None in (previous_fast, previous_slow, current_fast, current_slow):
        return None
    if previous_fast <= previous_slow and current_fast > current_slow:
        return "BUY"
    if previous_fast >= previous_slow and current_fast < current_slow:
        return "SELL"
    return None


__all__ = ["detect_cross", "ema_last", "ema_series", "rsi_series"]
