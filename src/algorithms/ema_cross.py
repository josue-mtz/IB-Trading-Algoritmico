"""Estrategia de cruce EMA con filtros opcionales de RSI y tendencia de largo plazo."""
from typing import Optional

from config.settings import (
    EMA_FAST, EMA_SLOW,
    RSI_PERIOD, RSI_OVERBOUGHT, RSI_OVERSOLD,
    EMA_TREND_PERIOD,
)
from src.algorithms.base_algorithm import BaseAlgorithm
from src.algorithms.indicators import ema_series, rsi_series, detect_cross


class EMACross(BaseAlgorithm):
    """Genera señales de compra o venta cuando la EMA rápida cruza la lenta."""

    id_algoritmo = "ema_cross_v1"

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
        # Conserva suficiente historial para los indicadores y sus periodos de calentamiento.
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

    def extra_status(self, candles: list) -> dict:
        """Devuelve precios recientes e indicadores para el panel en tiempo real."""
        closes = [float(candle["close"]) for candle in candles]
        fast = ema_series(closes, self.fast)
        slow = ema_series(closes, self.slow)
        trend = ema_series(closes, self.trend_period) if self.use_trend_filter else [None] * len(closes)
        rsi = rsi_series(closes, self.rsi_period) if self.use_rsi_filter else [None] * len(closes)

        def r(x):
            return None if x is None else round(x, 5)

        tail = slice(-60, None)
        series = [
            {
                "t": candle.get("inicio"),
                "close": r(close),
                "fast": r(fast_value),
                "slow": r(slow_value),
                "trend": r(trend_value),
            }
            for candle, close, fast_value, slow_value, trend_value
            in list(zip(candles, closes, fast, slow, trend))[tail]
        ]
        last_fast, last_slow = fast[-1], slow[-1]
        bias = None
        if last_fast is not None and last_slow is not None:
            bias = "BULLISH" if last_fast > last_slow else "BEARISH"
        return {
            "params": {"fast": self.fast, "slow": self.slow, "trend": self.trend_period,
                       "use_trend_filter": self.use_trend_filter, "use_rsi_filter": self.use_rsi_filter},
            "ema_fast": r(last_fast), "ema_slow": r(last_slow),
            "ema_trend": r(trend[-1]), "rsi": None if rsi[-1] is None else round(rsi[-1], 1),
            "bias": bias,
            "series": series,
        }

    def generate_signal(self, candles: list) -> Optional[dict]:
        """Evalúa la última vela y devuelve una señal si se cumplen todos los filtros."""
        if len(candles) < self._min_warmup():
            return None

        closes = [float(candle["close"]) for candle in candles]
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