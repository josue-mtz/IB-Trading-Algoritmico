"""Algoritmo de cruce de medias móviles exponenciales (EMA)."""
from typing import Optional

from config.settings import EMA_FAST, EMA_SLOW
from src.algorithms.base_algoritmo import BaseAlgorithm
from src.algorithms.indicadores import ema_series, detect_cross


class EMACross(BaseAlgorithm):
    id_algoritmo = "ema_cross_v1"

    def __init__(self, fast: int = EMA_FAST, slow: int = EMA_SLOW):
        # Buffer 3x el periodo lento para tener EMAs bien establecidas
        super().__init__(id_algoritmo=self.id_algoritmo, buffer_size=slow * 3)
        self.fast = fast
        self.slow = slow

    def generate_signal(self, velas: list) -> Optional[dict]:
        # Necesitamos al menos `slow + 1` velas para tener 2 valores válidos
        if len(velas) < self.slow + 1:
            return None

        closes = [float(v["close"]) for v in velas]
        fast_series = ema_series(closes, self.fast)
        slow_series = ema_series(closes, self.slow)

        prev_fast, curr_fast = fast_series[-2], fast_series[-1]
        prev_slow, curr_slow = slow_series[-2], slow_series[-1]

        signal = detect_cross(prev_fast, prev_slow, curr_fast, curr_slow)
        if signal is None:
            return None

        return {
            "signal": signal,
            "precio_referencia": closes[-1],
        }