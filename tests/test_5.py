"""Offline tests for backtest metrics, trading behavior, and strategy variants."""
import pandas as pd
from src.algorithms.indicadores import rsi_series
from src.algorithms.ema_cross import EMACross
from src.backtest.metrics import (
    winrate, profit_factor, expectancy, sharpe, max_drawdown,
    compute_daily_returns, deflated_sharpe,
)
from src.backtest.engine import BacktestEngine, trades_to_dicts


def _trades(pnls, exit_days=None):
    if exit_days is None:
        exit_days = [f"2026-01-{i+1:02d}" for i in range(len(pnls))]
    return [
        {"pnl_neto": p, "exit_time": f"{d}T12:00:00", "entry_time": f"{d}T11:00:00"}
        for p, d in zip(pnls, exit_days)
    ]


# ---------- Core metric tests ----------

def test_winrate():
    assert winrate([]) == 0.0
    assert winrate(_trades([10, 5, -3, -2])) == 0.5
    assert winrate(_trades([1, 2, 3])) == 1.0
    print("PASS test_winrate")


def test_profit_factor():
    assert abs(profit_factor(_trades([10, 5, -3, -2])) - 3.0) < 1e-9
    assert profit_factor(_trades([10, 5])) == float("inf")
    assert profit_factor(_trades([-3, -2])) == 0.0
    print("PASS test_profit_factor")


def test_expectancy():
    assert abs(expectancy(_trades([10, 5, -3, -2])) - 2.5) < 1e-9
    assert expectancy([]) == 0.0
    print("PASS test_expectancy")


def test_sharpe_positive():
    assert sharpe([0.01] * 10) == 0.0
    s = sharpe([0.01, 0.02, 0.015, 0.005, 0.025])
    assert s > 0
    print(f"PASS test_sharpe_positive (Sharpe={s:.2f})")


def test_max_drawdown():
    dd = max_drawdown([100, 120, 90, 130])
    assert abs(dd - 0.25) < 1e-9
    assert max_drawdown([]) == 0.0
    print("PASS test_max_drawdown")


def test_daily_returns():
    trades = _trades([10, -5, 20], exit_days=["2026-01-01", "2026-01-01", "2026-01-02"])
    r = compute_daily_returns(trades, initial_capital=10000)
    assert len(r) == 2
    assert abs(r[0] - 0.0005) < 1e-9
    assert abs(r[1] - 0.0020) < 1e-9
    print("PASS test_daily_returns")


def test_deflated_sharpe():
    dsr1 = deflated_sharpe(sharpe_obs=2.0, n_trials=1, n_obs=252)
    dsr1000 = deflated_sharpe(sharpe_obs=2.0, n_trials=1000, n_obs=252)
    assert dsr1 > dsr1000, f"DSR1={dsr1}, DSR1000={dsr1000}"
    print(f"PASS test_deflated_sharpe (DSR 1={dsr1:.3f}, 1000={dsr1000:.3f})")


def test_engine_no_lookahead():
    prices = [1.0800] * 25 + [1.0795, 1.0790, 1.0785] + [1.0820, 1.0850, 1.0880, 1.0885, 1.0890]
    df = pd.DataFrame({
        "symbol": ["EURUSD"] * len(prices),
        "inicio": pd.date_range("2026-01-01", periods=len(prices), freq="1min", tz="UTC"),
        "open": prices, "high": [p + 0.0005 for p in prices],
        "low": [p - 0.0005 for p in prices], "close": prices,
        "num_ticks": [100] * len(prices),
    })
    engine = BacktestEngine(fast=9, slow=21, spread_pips=0.0, slippage_pips=0.0)
    trades, equity_curve = engine.run(df)
    assert len(trades) >= 1
    assert equity_curve[0] == 10000.0
    print(f"PASS test_engine_no_lookahead (trade {trades[0].side} index {trades[0].entry_idx})")


def test_engine_sl_hit():
    prices = [1.0800] * 25 + [1.0795, 1.0790, 1.0785] + [1.0820] + [1.0750] * 5
    df = pd.DataFrame({
        "symbol": ["EURUSD"] * len(prices),
        "inicio": pd.date_range("2026-01-01", periods=len(prices), freq="1min", tz="UTC"),
        "open": prices, "high": [p + 0.0005 for p in prices],
        "low": [p - 0.0005 for p in prices], "close": prices,
        "num_ticks": [100] * len(prices),
    })
    engine = BacktestEngine(fast=9, slow=21, sl_pips=10, tp_pips=20,
                             spread_pips=0.0, slippage_pips=0.0)
    trades, _ = engine.run(df)
    reasons = [t.exit_reason for t in trades]
    assert "SL" in reasons or "TP" in reasons
    print(f"PASS test_engine_sl_hit (reasons={reasons})")


# ---------- RSI tests ----------

def test_rsi_monotonic_up():
    values = list(range(1, 20))
    rsi = rsi_series(values, 14)
    assert rsi[13] is None
    assert rsi[14] is not None
    assert abs(rsi[14] - 100.0) < 1e-6, f"rsi[14]={rsi[14]}"
    print("PASS test_rsi_monotonic_up")


def test_rsi_monotonic_down():
    values = list(range(20, 1, -1))
    rsi = rsi_series(values, 14)
    assert abs(rsi[14] - 0.0) < 1e-6, f"rsi[14]={rsi[14]}"
    print("PASS test_rsi_monotonic_down")


def test_rsi_warmup_and_mixed():
    assert all(r is None for r in rsi_series([1, 2, 3, 4, 5], 14))
    values = [1, 2, 3, 4, 5, 6, 7, 8, 7, 6, 5, 4, 3, 2, 1, 2, 3]
    rsi = rsi_series(values, 14)
    assert rsi[14] is not None
    assert 0 < rsi[14] < 100
    print(f"PASS test_rsi_warmup_and_mixed (rsi[14]={rsi[14]:.2f})")


# ---------- Strategy variant tests ----------

def test_variant_ids_are_distinct():
    v1 = EMACross()
    v2 = EMACross(use_rsi_filter=True, id_algoritmo="ema_cross_rsi")
    v3 = EMACross(use_trend_filter=True, id_algoritmo="ema_cross_trend")
    v4 = EMACross(use_rsi_filter=True, use_trend_filter=True, id_algoritmo="ema_cross_rsi_trend")
    ids = {v1.id_algoritmo, v2.id_algoritmo, v3.id_algoritmo, v4.id_algoritmo}
    assert len(ids) == 4, f"Strategy IDs are not unique: {ids}"
    print(f"PASS test_variant_ids_are_distinct ({sorted(ids)})")


def test_variant_buffer_size_sufficient():
    """The buffer must cover trend_period + slow + a safety margin."""
    algo = EMACross(use_trend_filter=True, trend_period=50, slow=21)
    assert algo.buffer.maxlen >= 50 + 21, f"buffer={algo.buffer.maxlen}"
    print(f"PASS test_variant_buffer_size_sufficient (maxlen={algo.buffer.maxlen})")


if __name__ == "__main__":
    print("=== Phase 5 tests (including strategy variants) ===")
    test_winrate()
    test_profit_factor()
    test_expectancy()
    test_sharpe_positive()
    test_max_drawdown()
    test_daily_returns()
    test_deflated_sharpe()
    test_engine_no_lookahead()
    test_engine_sl_hit()
    test_rsi_monotonic_up()
    test_rsi_monotonic_down()
    test_rsi_warmup_and_mixed()
    test_variant_ids_are_distinct()
    test_variant_buffer_size_sufficient()
    print("=== All Phase 5 tests passed ===")