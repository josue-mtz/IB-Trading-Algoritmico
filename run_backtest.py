"""Corre el backtest sobre IS/OOS para una variante y genera reporte."""
import argparse
import json
from dataclasses import fields
from datetime import datetime
import pandas as pd

from config.settings import REPORTS_DIR
from src.backtest.downloader import get_or_download
from src.backtest.engine import BacktestEngine, Trade, trades_to_dicts
from src.backtest.metrics import all_metrics, compute_daily_returns
from src.common.logger import get_logger

log = get_logger("backtest_runner")

INITIAL_CAPITAL = 10000.0
MINIMUM_CANDLES = 20_000
TRADE_COLUMNS = [field.name for field in fields(Trade)]

VARIANTS = {
    "v1": {"use_rsi_filter": False, "use_trend_filter": False, "id_algoritmo": "ema_cross_v1"},
    "v2": {"use_rsi_filter": True,  "use_trend_filter": False, "id_algoritmo": "ema_cross_rsi"},
    "v3": {"use_rsi_filter": False, "use_trend_filter": True,  "id_algoritmo": "ema_cross_trend"},
    "v4": {"use_rsi_filter": True,  "use_trend_filter": True,  "id_algoritmo": "ema_cross_rsi_trend"},
}


def run_backtest(variant: str = "v1", force_download: bool = False):
    if variant not in VARIANTS:
        raise ValueError(f"Variante desconocida: {variant}. Opciones: {list(VARIANTS)}")

    cfg = VARIANTS[variant]
    df = get_or_download(force=force_download)
    log.info(f"[{variant}] Dataset: {len(df)} velas | {df['inicio'].min()} → {df['inicio'].max()}")
    if len(df) < MINIMUM_CANDLES:
        raise RuntimeError(f"Dataset insuficiente: {len(df)} velas; se requieren {MINIMUM_CANDLES}.")

    split = int(len(df) * 0.7)
    df_is = df.iloc[:split].reset_index(drop=True)
    df_oos = df.iloc[split:].reset_index(drop=True)
    log.info(f"[{variant}] IS: {len(df_is)} | OOS: {len(df_oos)}")

    engine = BacktestEngine(
        initial_capital=INITIAL_CAPITAL,
        use_rsi_filter=cfg["use_rsi_filter"],
        use_trend_filter=cfg["use_trend_filter"],
    )
    trades_is, eq_is = engine.run(df_is)
    trades_oos, eq_oos = engine.run(df_oos)
    d_is = trades_to_dicts(trades_is)
    d_oos = trades_to_dicts(trades_oos)

    m_is = all_metrics(d_is, eq_is, compute_daily_returns(d_is, INITIAL_CAPITAL), INITIAL_CAPITAL)
    m_oos = all_metrics(d_oos, eq_oos, compute_daily_returns(d_oos, INITIAL_CAPITAL), INITIAL_CAPITAL)

    sharpe_degradation = None
    if m_is["sharpe"] > 0.3:
        sharpe_degradation = (m_is["sharpe"] - m_oos["sharpe"]) / m_is["sharpe"] * 100
        log.info(f"[{variant}] Degradación Sharpe: {sharpe_degradation:.1f}%")
    else:
        log.info(f"[{variant}] Degradación Sharpe: N/A (Sharpe IS={m_is['sharpe']:.2f})")

    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    out = REPORTS_DIR / f"backtest_{ts}_{variant}"
    out.mkdir(parents=True, exist_ok=True)

    payload = {
        "timestamp_utc": ts,
        "variant": variant,
        "id_algoritmo": cfg["id_algoritmo"],
        "params": {
            "fast": engine.fast, "slow": engine.slow,
            "sl_pips": engine.sl_pips, "tp_pips": engine.tp_pips,
            "spread_pips": engine.spread_pips, "slippage_pips": engine.slippage_pips,
            "use_rsi_filter": engine.use_rsi_filter,
            "use_trend_filter": engine.use_trend_filter,
            "rsi_period": engine.rsi_period,
            "rsi_overbought": engine.rsi_overbought,
            "rsi_oversold": engine.rsi_oversold,
            "trend_period": engine.trend_period,
            "initial_capital": INITIAL_CAPITAL,
        },
        "data_range": {"start": str(df["inicio"].min()), "end": str(df["inicio"].max()), "n_velas": len(df)},
        "in_sample": m_is,
        "out_of_sample": m_oos,
        "oos_vs_is": {
            "sharpe_degradation_pct": round(sharpe_degradation, 2) if sharpe_degradation is not None else None,
            "threshold_pct": 40,
            "applicable": sharpe_degradation is not None,
            "passed": (sharpe_degradation < 40) if sharpe_degradation is not None else None,
        },
    }

    pd.DataFrame(d_is, columns=TRADE_COLUMNS).to_csv(out / "trades_is.csv", index=False)
    pd.DataFrame(d_oos, columns=TRADE_COLUMNS).to_csv(out / "trades_oos.csv", index=False)

    import matplotlib.pyplot as plt
    fig, axes = plt.subplots(1, 2, figsize=(14, 5))
    axes[0].plot(eq_is); axes[0].set_title(f"IS [{variant}] ({len(trades_is)} trades) - PF={m_is['profit_factor']}")
    axes[1].plot(eq_oos); axes[1].set_title(f"OOS [{variant}] ({len(trades_oos)} trades) - PF={m_oos['profit_factor']}")
    for ax in axes:
        ax.set_xlabel("Trade #"); ax.set_ylabel("Equity (USD)")
        ax.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(out / "equity_curve.png", dpi=100)
    plt.close(fig)

    with open(out / "metrics.json", "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)

    log.info("=" * 60)
    log.info(f"RESULTADOS [{variant}] (id={cfg['id_algoritmo']})")
    log.info("-" * 60)
    for label, m in [("IS ", m_is), ("OOS", m_oos)]:
        log.info(
            f"{label}  trades={m['num_trades']:3d}  PF={m['profit_factor']}  "
            f"WR={m['winrate']*100:.1f}%  Exp={m['expectancy']:.2f}$  "
            f"Sharpe={m['sharpe']:.2f}  MaxDD={m['max_drawdown']*100:.1f}%  "
            f"PnL={m['total_pnl']}$"
        )
    log.info(f"Reporte: {out}")
    log.info("=" * 60)

    return m_is, m_oos, out


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--variant", default="v1", choices=list(VARIANTS))
    p.add_argument("--force-download", action="store_true")
    args = p.parse_args()
    run_backtest(variant=args.variant, force_download=args.force_download)