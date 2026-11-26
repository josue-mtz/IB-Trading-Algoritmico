"""
Motor de backtesting honesto.

Reglas anti-look-ahead:
  - La señal se genera con el cierre de la vela i.
  - La entrada se llena en la APERTURA de la vela i+1.
  - El spread y el slippage se aplican SIEMPRE en contra.
  - Si SL y TP se tocan en la misma vela, se asume SL (conservador).
"""
from dataclasses import dataclass, asdict
from typing import List, Tuple
import pandas as pd

from config.settings import (
    EMA_FAST, EMA_SLOW, BACKTEST_SPREAD_PIPS, BACKTEST_SLIPPAGE_PIPS,
    BACKTEST_SL_PIPS, BACKTEST_TP_PIPS,
)
from src.algorithms.indicadores import ema_series, detect_cross
from src.common.logger import get_logger

log = get_logger("backtest_engine")

PIP_SIZE = 0.0001
PIP_VALUE_USD_PER_10K = 1.0   # 1 pip = 1 USD en 10k EURUSD


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
    ):
        self.fast = fast
        self.slow = slow
        self.sl_pips = sl_pips
        self.tp_pips = tp_pips
        self.spread_pips = spread_pips
        self.slippage_pips = slippage_pips
        self.lot_units = lot_units
        self.initial_capital = initial_capital
        self._pip_value = PIP_VALUE_USD_PER_10K * (lot_units / 10000.0)

    def run(self, df: pd.DataFrame) -> Tuple[List[Trade], List[float]]:
        if len(df) < self.slow + 5:
            log.warning(f"Pocos datos ({len(df)} velas). Mínimo recomendado: {self.slow * 3}.")
            return [], [self.initial_capital]

        closes = df["close"].astype(float).tolist()
        opens = df["open"].astype(float).tolist()
        highs = df["high"].astype(float).tolist()
        lows = df["low"].astype(float).tolist()
        times = df["inicio"].astype(str).tolist()

        fast_s = ema_series(closes, self.fast)
        slow_s = ema_series(closes, self.slow)

        trades: List[Trade] = []
        total_pnl = 0.0
        equity = self.initial_capital
        equity_curve = [round(equity, 2)]

        spread_half = (self.spread_pips / 2) * PIP_SIZE
        slip = self.slippage_pips * PIP_SIZE

        i = self.slow + 1
        while i < len(df) - 1:
            signal = detect_cross(fast_s[i - 1], slow_s[i - 1], fast_s[i], slow_s[i])
            if signal is None:
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

            exit_price = None
            exit_reason = None
            exit_idx = None

            for j in range(entry_idx + 1, len(df)):
                if signal == "BUY":
                    # Conservador: si SL y TP en la misma vela, asume SL
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
                # fin de datos: cierra al último close con spread en contra
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
            total_pnl += pnl_usd
            equity += pnl_usd
            equity_curve.append(round(equity, 2))
            i = exit_idx + 1  # reanuda tras el cierre, sin solapar posiciones

        log.info(f"Backtest completo: {len(trades)} trades, P&L={total_pnl:.2f} USD")
        return trades, equity_curve


def trades_to_dicts(trades: List[Trade]) -> List[dict]:
    return [asdict(t) for t in trades]