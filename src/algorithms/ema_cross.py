"""Algoritmo de cruce de EMAs con filtros opcionales (RSI y tendencia)."""
from typing import Optional

from config.settings import (
    EMA_FAST, EMA_SLOW,
    RSI_PERIOD, RSI_OVERBOUGHT, RSI_OVERSOLD,
    EMA_TREND_PERIOD,
)
from src.algorithms.base_algoritmo import BaseAlgorithm
from src.algorithms.indicadores import ema_series, rsi_series, detect_cross


class EMACross(BaseAlgorithm):
    id_algoritmo = "ema_cross_v1"  # default; override via __init__

    def __init__(
        self,
        fast: int = EMA_FAST,
        slow: int = EMA_SLOW,
        use_rsi_filter: bool = False,
        use_trend_filter: bool = False,
        rsi_period: int = RSI_PERIOD,
        rsi_overbought: float = RSI_OVERBOUGHT,
        rsi_oversold: float = RSI_OVERSOLD,
        trend_period: int = EMA_TREND_PERIOD,
        id_algoritmo: str = "ema_cross_v1",
    ):
        # Buffer grande para cubrir slow EMA + trend EMA + margen
        buffer_size = max(slow * 3, slow + trend_period + 20)
        super().__init__(id_algoritmo=id_algoritmo, buffer_size=buffer_size)
        self.fast = fast
        self.slow = slow
        self.use_rsi_filter = use_rsi_filter
        self.use_trend_filter = use_trend_filter
        self.rsi_period = rsi_period
        self.rsi_overbought = rsi_overbought
        self.rsi_oversold = rsi_oversold
        self.trend_period = trend_period

    def _min_warmup(self) -> int:
        return max(
            self.slow,
            self.trend_period if self.use_trend_filter else 0,
            self.rsi_period if self.use_rsi_filter else 0,
        ) + 2

    def warmup_needed(self) -> int:
        return self._min_warmup()

    def extra_status(self, velas: list) -> dict:
        """Serie reciente (60 velas) de cierre y EMAs para graficar el modelo en vivo."""
        closes = [float(v["close"]) for v in velas]
        fast = ema_series(closes, self.fast)
        slow = ema_series(closes, self.slow)
        trend = ema_series(closes, self.trend_period) if self.use_trend_filter else [None] * len(closes)
        rsi = rsi_series(closes, self.rsi_period) if self.use_rsi_filter else [None] * len(closes)

        def r(x):
            return None if x is None else round(x, 5)

        tail = slice(-60, None)
        series = [
            {"t": v.get("inicio"), "close": r(c), "fast": r(f), "slow": r(sl), "trend": r(t)}
            for v, c, f, sl, t in list(zip(velas, closes, fast, slow, trend))[tail]
        ]
        last_fast, last_slow = fast[-1], slow[-1]
        bias = None
        if last_fast is not None and last_slow is not None:
            bias = "ALCISTA" if last_fast > last_slow else "BAJISTA"
        return {
            "params": {"fast": self.fast, "slow": self.slow, "trend": self.trend_period,
                       "use_trend_filter": self.use_trend_filter, "use_rsi_filter": self.use_rsi_filter},
            "ema_fast": r(last_fast), "ema_slow": r(last_slow),
            "ema_trend": r(trend[-1]), "rsi": None if rsi[-1] is None else round(rsi[-1], 1),
            "bias": bias,
            "series": series,
        }

    def generate_signal(self, velas: list) -> Optional[dict]:
        if len(velas) < self._min_warmup():
            return None

        closes = [float(v["close"]) for v in velas]
        fast_s = ema_series(closes, self.fast)
        slow_s = ema_series(closes, self.slow)

        prev_fast, curr_fast = fast_s[-2], fast_s[-1]
        prev_slow, curr_slow = slow_s[-2], slow_s[-1]
        signal = detect_cross(prev_fast, prev_slow, curr_fast, curr_slow)
        if signal is None:
            return None

        if self.use_rsi_filter:
            rsi_val = rsi_series(closes, self.rsi_period)[-1]
            if rsi_val is not None:
                if signal == "BUY" and rsi_val > self.rsi_overbought:
                    return None
                if signal == "SELL" and rsi_val < self.rsi_oversold:
                    return None

        if self.use_trend_filter:
            trend_val = ema_series(closes, self.trend_period)[-1]
            if trend_val is not None:
                if signal == "BUY" and closes[-1] < trend_val:
                    return None
                if signal == "SELL" and closes[-1] > trend_val:
                    return None

        return {"signal": signal, "precio_referencia": closes[-1]}