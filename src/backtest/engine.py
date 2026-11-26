"""
Motor de backtesting honesto con soporte para filtros RSI y tendencia.
Reglas anti-look-ahead:
  - La señal se genera con el cierre de la vela i.
  - La entrada se llena en la APERTURA de la vela i+1.
  - Spread y slippage SIEMPRE en contra.
  - Si SL y TP se tocan en la misma vela, se asume SL.
"""
from dataclasses import dataclass, asdict
from typing import List, Optional, Tuple
import pandas as pd

from config.settings import (
    EMA_FAST, EMA_SLOW, BACKTEST_SPREAD_PIPS, BACKTEST_SLIPPAGE_PIPS,
    BACKTEST_SL_PIPS, BACKTEST_TP_PIPS,
    RSI_PERIOD, RSI_OVERBOUGHT, RSI_OVERSOLD, EMA_TREND_PERIOD,
)
from src.algorithms.indicadores import ema_series, rsi_series, detect_cross
from src.common.logger import get_logger

log = get_logger("backtest_engine")

PIP_SIZE = 0.0001
PIP_VALUE_USD_PER_10K = 1.0


@dataclass
class Trade:
    entry_idx: int
    exit_idx: int
    side: str
    entry_price: float
    exit_price: float
    sl_price: float
    tp_price: float
    pnl_pips: float
    pnl_neto: float
    exit_reason: str
    entry_time: str
    exit_time: str


class BacktestEngine:
    def __init__(
        self,
        fast: int = EMA_FAST,
        slow: int = EMA_SLOW,
        sl_pips: float = BACKTEST_SL_PIPS,
        tp_pips: float = BACKTEST_TP_PIPS,
        spread_pips: float = BACKTEST_SPREAD_PIPS,
        slippage_pips: float = BACKTEST_SLIPPAGE_PIPS,
        lot_units: int = 10000,
        initial_capital: float = 10000.0,
        use_rsi_filter: bool = False,
        use_trend_filter: bool = False,
        rsi_period: int = RSI_PERIOD,
        rsi_overbought: float = RSI_OVERBOUGHT,
        rsi_oversold: float = RSI_OVERSOLD,
        trend_period: int = EMA_TREND_PERIOD,
    ):
        self.fast = fast
        self.slow = slow
        self.sl_pips = sl_pips
        self.tp_pips = tp_pips
        self.spread_pips = spread_pips
        self.slippage_pips = slippage_pips
        self.lot_units = lot_units
        self.initial_capital = initial_capital
        self.use_rsi_filter = use_rsi_filter
        self.use_trend_filter = use_trend_filter
        self.rsi_period = rsi_period
        self.rsi_overbought = rsi_overbought
        self.rsi_oversold = rsi_oversold
        self.trend_period = trend_period
        self._pip_value = PIP_VALUE_USD_PER_10K * (lot_units / 10000.0)

    def _min_warmup(self) -> int:
        return max(
            self.slow,
            self.trend_period if self.use_trend_filter else 0,
            self.rsi_period if self.use_rsi_filter else 0,
        ) + 2

    def _passes_filters(self, side, i, closes, rsi_s, trend_s):
        if self.use_rsi_filter and rsi_s is not None:
            r = rsi_s[i]
            if r is not None:
                if side == "BUY" and r > self.rsi_overbought:
                    return False
                if side == "SELL" and r < self.rsi_oversold:
                    return False
        if self.use_trend_filter and trend_s is not None:
            t = trend_s[i]
            if t is not None:
                if side == "BUY" and closes[i] < t:
                    return False
                if side == "SELL" and closes[i] > t:
                    return False
        return True

    def run(
        self,
        df: pd.DataFrame,
        start_index: Optional[int] = None,
    ) -> Tuple[List[Trade], List[float]]:
        min_warmup = self._min_warmup()
        if len(df) < min_warmup:
            log.warning(f"Pocos datos ({len(df)} velas). Mínimo: {min_warmup}.")
            return [], [self.initial_capital]

        closes = df["close"].astype(float).tolist()
        opens = df["open"].astype(float).tolist()
        highs = df["high"].astype(float).tolist()
        lows = df["low"].astype(float).tolist()
        times = df["inicio"].astype(str).tolist()

        fast_s = ema_series(closes, self.fast)
        slow_s = ema_series(closes, self.slow)
        rsi_s = rsi_series(closes, self.rsi_period) if self.use_rsi_filter else None
        trend_s = ema_series(closes, self.trend_period) if self.use_trend_filter else None

        trades: List[Trade] = []
        equity = self.initial_capital
        equity_curve = [round(equity, 2)]
        spread_half = (self.spread_pips / 2) * PIP_SIZE
        slip = self.slippage_pips * PIP_SIZE

        first_signal_index = min_warmup if start_index is None else start_index
        i = max(min_warmup, first_signal_index)
        if i >= len(df) - 1:
            return [], [self.initial_capital]
        while i < len(df) - 1:
            signal = detect_cross(fast_s[i - 1], slow_s[i - 1], fast_s[i], slow_s[i])
            if signal is None or not self._passes_filters(signal, i, closes, rsi_s, trend_s):
                i += 1
                continue

            entry_idx = i + 1
            if entry_idx >= len(df):
                break

            if signal == "BUY":
                entry_price = opens[entry_idx] + spread_half + slip
                sl_price = entry_price - self.sl_pips * PIP_SIZE
                tp_price = entry_price + self.tp_pips * PIP_SIZE
            else:
                entry_price = opens[entry_idx] - spread_half - slip
                sl_price = entry_price + self.sl_pips * PIP_SIZE
                tp_price = entry_price - self.tp_pips * PIP_SIZE

            exit_price = exit_reason = exit_idx = None
            for j in range(entry_idx + 1, len(df)):
                if signal == "BUY":
                    if lows[j] <= sl_price:
                        exit_price, exit_reason, exit_idx = sl_price, "SL", j
                        break
                    if highs[j] >= tp_price:
                        exit_price, exit_reason, exit_idx = tp_price, "TP", j
                        break
                else:
                    if highs[j] >= sl_price:
                        exit_price, exit_reason, exit_idx = sl_price, "SL", j
                        break
                    if lows[j] <= tp_price:
                        exit_price, exit_reason, exit_idx = tp_price, "TP", j
                        break

            if exit_price is None:
                exit_idx = len(df) - 1
                exit_price = (closes[-1] - spread_half - slip) if signal == "BUY" \
                             else (closes[-1] + spread_half + slip)
                exit_reason = "EOD"

            pnl_pips = ((exit_price - entry_price) if signal == "BUY"
                        else (entry_price - exit_price)) / PIP_SIZE
            pnl_usd = pnl_pips * self._pip_value

            trades.append(Trade(
                entry_idx=entry_idx, exit_idx=exit_idx, side=signal,
                entry_price=round(entry_price, 5), exit_price=round(exit_price, 5),
                sl_price=round(sl_price, 5), tp_price=round(tp_price, 5),
                pnl_pips=round(pnl_pips, 2), pnl_neto=round(pnl_usd, 2),
                exit_reason=exit_reason,
                entry_time=times[entry_idx], exit_time=times[exit_idx],
            ))
            equity += pnl_usd
            equity_curve.append(round(equity, 2))
            i = exit_idx + 1

        log.info(f"Backtest completo: {len(trades)} trades, P&L={equity - self.initial_capital:.2f} USD")
        return trades, equity_curve


def trades_to_dicts(trades: List[Trade]) -> List[dict]:
    return [asdict(t) for t in trades]