"""
Indicadores técnicos puros (sin efectos secundarios).
Se reutilizarán en Fase 5 (backtest) y Fase 6 (genético).
"""
from typing import List, Optional


def ema_series(values: List[float], period: int) -> List[Optional[float]]:
    """
    Calcula la serie completa de EMA usando SMA como semilla.

    Retorna una lista de la MISMA longitud que `values`, donde los primeros
    (period - 1) elementos son None (periodo de calentamiento). Desde el
    índice (period - 1) en adelante, cada valor es la EMA válida.
    """
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
    """Devuelve solo el último valor de la EMA (o None si no hay suficientes datos)."""
    return ema_series(values, period)[-1]


def detect_cross(
    prev_fast: Optional[float],
    prev_slow: Optional[float],
    curr_fast: Optional[float],
    curr_slow: Optional[float],
) -> Optional[str]:
    """
    Detecta cruce entre dos EMAs.

    - BUY:  antes fast <= slow, ahora fast >  slow
    - SELL: antes fast >= slow, ahora fast <  slow
    - None: sin cruce o datos insuficientes
    """
    if None in (prev_fast, prev_slow, curr_fast, curr_slow):
        return None
    if prev_fast <= prev_slow and curr_fast > curr_slow:
        return "BUY"
    if prev_fast >= prev_slow and curr_fast < curr_slow:
        return "SELL"
    return None