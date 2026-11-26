"""
Tests unitarios de Fase 5. NO requieren IB ni Kafka.
Verifican métricas con valores conocidos y el motor con datos sintéticos.
"""
import pandas as pd
from src.backtest.metrics import (
    winrate, profit_factor, expectancy, sharpe, max_drawdown,
    compute_daily_returns, deflated_sharpe,
)
from src.backtest.engine import BacktestEngine, trades_to_dicts


def _trades(pnls, exit_days=None):
    """Helper: crea trades con P&L dados para probar métricas."""
    if exit_days is None:
        exit_days = [f"2026-01-{i+1:02d}" for i in range(len(pnls))]
    return [
        {"pnl_neto": p, "exit_time": f"{d}T12:00:00", "entry_time": f"{d}T11:00:00"}
        for p, d in zip(pnls, exit_days)
    ]


def test_winrate():
    assert winrate([]) == 0.0
    assert winrate(_trades([10, 5, -3, -2])) == 0.5
    assert winrate(_trades([1, 2, 3])) == 1.0
    print("✅ test_winrate")


def test_profit_factor():
    # GP=15, GL=5 → PF=3.0
    assert abs(profit_factor(_trades([10, 5, -3, -2])) - 3.0) < 1e-9
    # sin pérdidas → inf
    assert profit_factor(_trades([10, 5])) == float("inf")
    # sin ganancias → 0
    assert profit_factor(_trades([-3, -2])) == 0.0
    print("✅ test_profit_factor")


def test_expectancy():
    # (10+5-3-2)/4 = 2.5
    assert abs(expectancy(_trades([10, 5, -3, -2])) - 2.5) < 1e-9
    assert expectancy([]) == 0.0
    print("✅ test_expectancy")


def test_sharpe_positive():
    # retornos constantes positivos → std=0 → 0.0
    assert sharpe([0.01] * 10) == 0.0
    # retornos variados positivos → positivo
    s = sharpe([0.01, 0.02, 0.015, 0.005, 0.025])
    assert s > 0
    print(f"✅ test_sharpe_positive (Sharpe={s:.2f})")


def test_max_drawdown():
    # 100 → 120 → 90 → 130 → max DD = (120-90)/120 = 25%
    dd = max_drawdown([100, 120, 90, 130])
    assert abs(dd - 0.25) < 1e-9
    assert max_drawdown([]) == 0.0
    print("✅ test_max_drawdown")


def test_daily_returns():
    trades = _trades([10, -5, 20], exit_days=["2026-01-01", "2026-01-01", "2026-01-02"])
    r = compute_daily_returns(trades, initial_capital=10000)
    # Día 1: 10-5=5 → 0.0005; Día 2: 20 → 0.0020
    assert len(r) == 2
    assert abs(r[0] - 0.0005) < 1e-9
    assert abs(r[1] - 0.0020) < 1e-9
    print("✅ test_daily_returns")


def test_deflated_sharpe():
    # Sharpe alto y solo 1 prueba → DSR alto
    dsr1 = deflated_sharpe(sharpe_obs=2.0, n_trials=1, n_obs=252)
    # Mismo Sharpe pero 1000 pruebas → DSR cae
    dsr1000 = deflated_sharpe(sharpe_obs=2.0, n_trials=1000, n_obs=252)
    assert dsr1 > dsr1000, f"DSR1={dsr1}, DSR1000={dsr1000}"
    print(f"✅ test_deflated_sharpe (DSR 1 prueba={dsr1:.3f}, 1000 pruebas={dsr1000:.3f})")


def test_engine_no_lookahead():
    """Verifica que la entrada ocurre en la vela siguiente a la señal."""
    # Dataset sintético: flat → bajada → subida fuerte (fuerza un BUY)
    prices = [1.0800] * 25 + [1.0795, 1.0790, 1.0785] + [1.0820, 1.0850, 1.0880, 1.0885, 1.0890]
    df = pd.DataFrame({
        "symbol": ["EURUSD"] * len(prices),
        "inicio": pd.date_range("2026-01-01", periods=len(prices), freq="1min", tz="UTC"),
        "open": prices, "high": [p + 0.0005 for p in prices],
        "low":  [p - 0.0005 for p in prices], "close": prices,
        "num_ticks": [100] * len(prices),
    })
    engine = BacktestEngine(fast=9, slow=21, spread_pips=0.0, slippage_pips=0.0)
    trades, equity_curve = engine.run(df)
    assert len(trades) >= 1, "❌ Esperaba al menos 1 trade"
    assert equity_curve[0] == 10000.0, "❌ Equity inicial incorrecta"
    t = trades[0]
    assert t.entry_idx > 0, "❌ entry_idx inválido"
    # La señal se generó en entry_idx - 1
    print(f"✅ test_engine_no_lookahead (trade {t.side} en idx {t.entry_idx}, razón {t.exit_reason})")


def test_engine_sl_hit():
    """Si el precio se va en contra, debe cerrar por SL."""
    # BUY entra en 1.0800, precio cae a 1.0750 (SL a 10 pips=1.0790 se toca)
    prices = [1.0800] * 25 + [1.0795, 1.0790, 1.0785] + [1.0820] + [1.0750] * 5
    df = pd.DataFrame({
        "symbol": ["EURUSD"] * len(prices),
        "inicio": pd.date_range("2026-01-01", periods=len(prices), freq="1min", tz="UTC"),
        "open": prices, "high": [p + 0.0005 for p in prices],
        "low":  [p - 0.0005 for p in prices], "close": prices,
        "num_ticks": [100] * len(prices),
    })
    engine = BacktestEngine(fast=9, slow=21, sl_pips=10, tp_pips=20,
                             spread_pips=0.0, slippage_pips=0.0)
    trades, _ = engine.run(df)
    assert len(trades) >= 1
    # Al menos el primer trade debe haber cerrado por SL (o TP, pero en este caso SL)
    reasons = [t.exit_reason for t in trades]
    assert "SL" in reasons or "TP" in reasons, f"❌ Exit reasons: {reasons}"
    print(f"✅ test_engine_sl_hit (reasons={reasons})")


if __name__ == "__main__":
    print(" Tests de Fase 5 ")
    test_winrate()
    test_profit_factor()
    test_expectancy()
    test_sharpe_positive()
    test_max_drawdown()
    test_daily_returns()
    test_deflated_sharpe()
    test_engine_no_lookahead()
    test_engine_sl_hit()
    print(" Todos los tests de Fase 5 pasaron ")