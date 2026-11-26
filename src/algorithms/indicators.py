"""Cálculos puros de indicadores técnicos compartidos por trading y backtests."""
from typing import List, Optional


def ema_series(values: List[float], period: int) -> List[Optional[float]]:
    """Devuelve una serie de medias móviles exponenciales alineada con ``values``.

    Las primeras ``period - 1`` entradas son ``None``. El primer valor se
    calcula con una media móvil simple; los siguientes usan la fórmula EMA estándar.
    """
    if period < 1:
        raise ValueError("el periodo debe ser al menos 1")
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
    """Devuelve el último valor EMA o ``None`` si no hay datos suficientes."""
    series = ema_series(values, period)
    return series[-1] if series else None


def rsi_series(values: List[float], period: int = 14) -> List[Optional[float]]:
    """Devuelve el índice de fuerza relativa de Wilder alineado con ``values``.

    Las primeras ``period`` entradas son ``None`` porque cada valor RSI requiere
    ``period`` cambios de precio. Un intervalo sin cambios produce un RSI de 50.
    """
    if period < 1:
        raise ValueError("el periodo debe ser al menos 1")
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
    """Devuelve ``BUY`` o ``SELL`` cuando se cruzan dos medias móviles."""
    if None in (previous_fast, previous_slow, current_fast, current_slow):
        return None
    if previous_fast <= previous_slow and current_fast > current_slow:
        return "BUY"
    if previous_fast >= previous_slow and current_fast < current_slow:
        return "SELL"
    return None


__all__ = ["detect_cross", "ema_last", "ema_series", "rsi_series"]
