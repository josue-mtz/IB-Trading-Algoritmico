"""
Corre las 4 variantes sobre el MISMO dataset (carga una vez) y muestra tabla
comparativa. No re-descarga.
"""
import json
from datetime import datetime
import pandas as pd

from config.settings import REPORTS_DIR
from run_backtest import run_backtest, VARIANTS
from src.backtest.downloader import get_or_download
from src.backtest.engine import BacktestEngine, trades_to_dicts
from src.backtest.metrics import all_metrics, compute_daily_returns
from src.common.logger import get_logger

log = get_logger("variants_runner")
INITIAL_CAPITAL = 10000.0


def main():
    df = get_or_download()
    if len(df) < 20_000:
        raise RuntimeError(f"Dataset insuficiente: {len(df)} velas.")

    split = int(len(df) * 0.7)
    df_is = df.iloc[:split].reset_index(drop=True)
    df_oos = df.iloc[split:].reset_index(drop=True)
    log.info(f"Dataset: {len(df)} velas | IS {len(df_is)} | OOS {len(df_oos)}")

    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    summary_root = REPORTS_DIR / f"variants_comparison_{ts}"
    summary_root.mkdir(parents=True, exist_ok=True)

    rows = []
    for name, cfg in VARIANTS.items():
        engine = BacktestEngine(
            initial_capital=INITIAL_CAPITAL,
            use_rsi_filter=cfg["use_rsi_filter"],
            use_trend_filter=cfg["use_trend_filter"],
        )
        t_is, eq_is = engine.run(df_is)
        t_oos, eq_oos = engine.run(df_oos)
        d_is = trades_to_dicts(t_is)
        d_oos = trades_to_dicts(t_oos)
        m_is = all_metrics(d_is, eq_is, compute_daily_returns(d_is, INITIAL_CAPITAL), INITIAL_CAPITAL)
        m_oos = all_metrics(d_oos, eq_oos, compute_daily_returns(d_oos, INITIAL_CAPITAL), INITIAL_CAPITAL)

        rows.append({
            "variant": name,
            "id_algoritmo": cfg["id_algoritmo"],
            "is_trades": m_is["num_trades"],
            "is_pf": m_is["profit_factor"],
            "is_wr_pct": round(m_is["winrate"] * 100, 1),
            "is_sharpe": m_is["sharpe"],
            "is_maxdd_pct": round(m_is["max_drawdown"] * 100, 1),
            "is_pnl_usd": m_is["total_pnl"],
            "oos_trades": m_oos["num_trades"],
            "oos_pf": m_oos["profit_factor"],
            "oos_wr_pct": round(m_oos["winrate"] * 100, 1),
            "oos_sharpe": m_oos["sharpe"],
            "oos_maxdd_pct": round(m_oos["max_drawdown"] * 100, 1),
            "oos_pnl_usd": m_oos["total_pnl"],
        })

    df_out = pd.DataFrame(rows)
    df_out.to_csv(summary_root / "comparison.csv", index=False)
    with open(summary_root / "comparison.json", "w", encoding="utf-8") as f:
        json.dump(rows, f, indent=2)

    # Tabla en consola
    log.info("=" * 110)
    log.info("COMPARATIVA DE VARIANTES")
    log.info("=" * 110)
    log.info(f"{'Var':<5}{'Trades':>8}{'PF':>8}{'WR%':>8}{'Sharpe':>9}{'MaxDD%':>9}{'PnL$':>10}  |"
             f"{'Trades':>8}{'PF':>8}{'WR%':>8}{'Sharpe':>9}{'MaxDD%':>9}{'PnL$':>10}")
    log.info(f"{'':5}{'IS':>44}  |{'OOS':>44}")
    log.info("-" * 110)
    for r in rows:
        log.info(
            f"{r['variant']:<5}{r['is_trades']:>8}{str(r['is_pf']):>8}{r['is_wr_pct']:>8}"
            f"{r['is_sharpe']:>9}{r['is_maxdd_pct']:>9}{r['is_pnl_usd']:>10}  |"
            f"{r['oos_trades']:>8}{str(r['oos_pf']):>8}{r['oos_wr_pct']:>8}"
            f"{r['oos_sharpe']:>9}{r['oos_maxdd_pct']:>9}{r['oos_pnl_usd']:>10}"
        )
    log.info("=" * 110)

    # Ganador por Sharpe OOS
    winners = [r for r in rows if (r["oos_sharpe"] or 0) > 0]
    if winners:
        best = max(winners, key=lambda r: r["oos_sharpe"])
        log.info(f"🏆 Mejor variante por Sharpe OOS: {best['variant']} ({best['id_algoritmo']}) → Sharpe={best['oos_sharpe']}")
        log.info(f"   Para activar en vivo: actualiza EXECUTOR_ALLOWED_ALGORITHMS={best['id_algoritmo']} en .env")
    else:
        log.info("❌ Ninguna variante tiene Sharpe OOS positivo. Recomendación: pasar a Opción A (GA).")

    log.info(f"Reporte: {summary_root}")


if __name__ == "__main__":
    main()