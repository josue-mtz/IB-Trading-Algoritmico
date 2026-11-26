"""Métricas de backtesting. Todas puras, testeables sin red."""
from typing import List
import numpy as np


def winrate(trades: List[dict]) -> float:
    if not trades:
        return 0.0
    return sum(1 for t in trades if t["pnl_neto"] > 0) / len(trades)


def profit_factor(trades: List[dict]) -> float:
    gp = sum(t["pnl_neto"] for t in trades if t["pnl_neto"] > 0)
    gl = abs(sum(t["pnl_neto"] for t in trades if t["pnl_neto"] < 0))
    if gl == 0:
        return float("inf") if gp > 0 else 0.0
    return gp / gl


def expectancy(trades: List[dict]) -> float:
    if not trades:
        return 0.0
    return sum(t["pnl_neto"] for t in trades) / len(trades)


def sharpe(daily_returns: List[float], risk_free_annual: float = 0.0) -> float:
    """Sharpe anualizado a partir de retornos diarios."""
    if len(daily_returns) < 2:
        return 0.0
    r = np.array(daily_returns, dtype=float)
    excess = r - risk_free_annual / 252
    std = excess.std(ddof=1)
    if np.isclose(std, 0.0, atol=1e-12):
        return 0.0
    return float(excess.mean() / std * np.sqrt(252))


def max_drawdown(equity_curve: List[float]) -> float:
    """Max Drawdown como fracción (0.15 = 15%)."""
    if not equity_curve:
        return 0.0
    peak = equity_curve[0]
    max_dd = 0.0
    for v in equity_curve:
        if v > peak:
            peak = v
        if peak > 0:
            dd = (peak - v) / peak
            if dd > max_dd:
                max_dd = dd
    return max_dd


def deflated_sharpe(sharpe_obs: float, n_trials: int, n_obs: int,
                     skew: float = 0.0, kurt: float = 3.0) -> float:
    """Approximate Deflated Sharpe probability for a per-observation Sharpe."""
    if n_obs < 2 or n_trials < 1:
        return 0.0
    from math import sqrt, log, exp, pi
    from statistics import NormalDist
    e_max = sqrt(2 * log(n_trials)) if n_trials > 1 else 0.0
    # varianza del estimador de Sharpe
    var_sr = (1 - skew * sharpe_obs + ((kurt - 1) / 4) * sharpe_obs ** 2) / (n_obs - 1)
    if var_sr <= 0:
        return 0.0
    # probabilidad de que el Sharpe observado supere el máximo esperado por azar
    z = (sharpe_obs - e_max) / sqrt(var_sr)
    return float(NormalDist().cdf(z))


def compute_daily_returns(trades: List[dict], initial_capital: float = 10000.0) -> List[float]:
    """Agrupa P&L por día (usando exit_time) y lo normaliza por capital inicial."""
    if not trades:
        return []
    by_day = {}
    for t in trades:
        day = t["exit_time"][:10]
        by_day[day] = by_day.get(day, 0.0) + t["pnl_neto"]
    return [by_day[d] / initial_capital for d in sorted(by_day)]


def all_metrics(trades: List[dict], equity_curve: List[float],
                 daily_returns: List[float], initial_capital: float = 10000.0) -> dict:
    return {
        "num_trades": len(trades),
        "winrate": round(winrate(trades), 4),
        "profit_factor": round(profit_factor(trades), 4) if profit_factor(trades) != float("inf") else None,
        "expectancy": round(expectancy(trades), 4),
        "sharpe": round(sharpe(daily_returns), 4),
        "max_drawdown": round(max_drawdown(equity_curve), 4),
        "total_pnl": round(sum(t["pnl_neto"] for t in trades), 2) if trades else 0.0,
        "final_equity": round(initial_capital + sum(t["pnl_neto"] for t in trades), 2) if trades else initial_capital,
    }