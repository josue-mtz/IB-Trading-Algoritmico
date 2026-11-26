"""
Indicadores técnicos puros (sin efectos secundarios).
Se reutilizan en Fase 5 (backtest) y Fase 6 (genético).
"""
from typing import List, Optional


def ema_series(values: List[float], period: int) -> List[Optional[float]]:
    n = len(values)
    if n < period:
        return [None] * n
    result: List[Optional[float]] = [None] * (period - 1)
    sma_seed = sum(values[:period]) / period
    result.append(sma_seed)
    alpha = 2.0 / (period + 1)
    for i in range(period, n):
        ema_prev = result[-1]
        result.append(alpha * values[i] + (1 - alpha) * ema_prev)
    return result


def ema_last(values: List[float], period: int) -> Optional[float]:
    return ema_series(values, period)[-1]


def rsi_series(values: List[float], period: int = 14) -> List[Optional[float]]:
    """
    RSI de Wilder. Retorna lista de la misma longitud que `values`.
    Los primeros `period` elementos son None (warmup).

    Convención para avg_loss == 0:
      - Si hubo solo ganancias → RSI = 100.0
      - Si hubo solo pérdidas → RSI = 0.0
    """
    n = len(values)
    if period < 1 or n < period + 1:
        return [None] * n

    result: List[Optional[float]] = [None] * n
    gains = [0.0] * n
    losses = [0.0] * n
    for i in range(1, n):
        delta = values[i] - values[i - 1]
        if delta > 0:
            gains[i] = delta
        else:
            losses[i] = -delta

    def _rsi(g: float, l: float) -> float:
        if l == 0:
            return 100.0 if g > 0 else 50.0
        rs = g / l
        return 100.0 - 100.0 / (1.0 + rs)

    avg_gain = sum(gains[1: period + 1]) / period
    avg_loss = sum(losses[1: period + 1]) / period
    result[period] = _rsi(avg_gain, avg_loss)

    for i in range(period + 1, n):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
        result[i] = _rsi(avg_gain, avg_loss)

    return result


def detect_cross(
    prev_fast: Optional[float],
    prev_slow: Optional[float],
    curr_fast: Optional[float],
    curr_slow: Optional[float],
) -> Optional[str]:
    if None in (prev_fast, prev_slow, curr_fast, curr_slow):
        return None
    if prev_fast <= prev_slow and curr_fast > curr_slow:
        return "BUY"
    if prev_fast >= prev_slow and curr_fast < curr_slow:
        return "SELL"
    return None