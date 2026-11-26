"""Optimize EMA strategy parameters on IS and compare once on the final OOS."""
import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from config.settings import (
    BACKTEST_SL_PIPS,
    BACKTEST_TP_PIPS,
    DATA_DIR,
    EMA_FAST,
    EMA_SLOW,
    EMA_TREND_PERIOD,
    REPORTS_DIR,
)
from src.backtest.genetic_optimizer import (
    Candidate,
    GeneticOptimizer,
    evaluate_window,
)
from src.common.logger import get_logger

log = get_logger("genetic_backtest")
INITIAL_CAPITAL = 10000.0
MINIMUM_CANDLES = 20000
REQUIRED_COLUMNS = {"symbol", "inicio", "open", "high", "low", "close", "num_ticks"}


def run_ga_comparison(
    csv_path: Path = DATA_DIR / "EURUSD_M1.csv",
    output_root: Path = REPORTS_DIR,
    population_size: int = 20,
    generations: int = 8,
    seed: int = 42,
) -> Path:
    data = pd.read_csv(csv_path)
    missing_columns = REQUIRED_COLUMNS.difference(data.columns)
    if missing_columns:
        raise ValueError(f"CSV sin columnas requeridas: {sorted(missing_columns)}")
    if len(data) < MINIMUM_CANDLES:
        raise ValueError(
            f"Dataset insuficiente: {len(data)} velas; se requieren {MINIMUM_CANDLES}."
        )
    data["inicio"] = pd.to_datetime(data["inicio"], utc=True)
    if data["inicio"].isna().any():
        raise ValueError("El CSV contiene timestamps inválidos en la columna 'inicio'.")
    if not data["inicio"].is_monotonic_increasing:
        raise ValueError("El CSV debe estar ordenado cronológicamente por 'inicio'.")

    split = int(len(data) * 0.7)
    is_data = data.iloc[:split].reset_index(drop=True)
    oos_start = split
    log.info(
        "Dataset: %s velas | IS=%s | OOS=%s",
        len(data),
        len(is_data),
        len(data) - oos_start,
    )

    optimizer = GeneticOptimizer(
        is_data,
        population_size=population_size,
        generations=generations,
        seed=seed,
        initial_capital=INITIAL_CAPITAL,
    )
    ga_result = optimizer.optimize()
    ga_candidate = Candidate(**ga_result["candidate"])
    baseline = Candidate(
        fast=EMA_FAST,
        slow=EMA_SLOW,
        rsi_level=70,
        sl_pips=int(BACKTEST_SL_PIPS),
        tp_pips=int(BACKTEST_TP_PIPS),
    )
    baseline_is = optimizer.evaluate_fixed(
        baseline,
        use_rsi_filter=False,
        n_trials=1,
    )

    # OOS is evaluated only after selection; its result must not influence the GA.
    ga_oos = evaluate_window(
        data,
        ga_candidate,
        start_index=oos_start,
        n_trials=optimizer.trial_budget,
        initial_capital=INITIAL_CAPITAL,
        use_rsi_filter=True,
    )
    baseline_oos = evaluate_window(
        data,
        baseline,
        start_index=oos_start,
        n_trials=1,
        initial_capital=INITIAL_CAPITAL,
        use_rsi_filter=False,
    )

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    output_dir = Path(output_root) / f"genetic_comparison_{timestamp}"
    output_dir.mkdir(parents=True, exist_ok=False)

    comparisons = []
    for label, candidate, is_result, oos_result, use_rsi_filter in (
        ("V3_baseline", baseline, baseline_is, baseline_oos, False),
        ("GA_optimized", ga_candidate, ga_result, ga_oos, True),
    ):
        for segment, result in (("IS_walk_forward", is_result), ("OOS", oos_result)):
            metrics = result["metrics"]
            comparisons.append({
                "strategy": label,
                "segment": segment,
                **metrics,
                "deflated_sharpe": result["deflated_sharpe"],
                "candidate": json.dumps(candidate.__dict__, sort_keys=True),
                "use_rsi_filter": use_rsi_filter,
            })

    pd.DataFrame(comparisons).to_csv(output_dir / "comparison.csv", index=False)
    pd.DataFrame(ga_oos["trades"]).to_csv(output_dir / "trades_ga_oos.csv", index=False)
    pd.DataFrame(baseline_oos["trades"]).to_csv(
        output_dir / "trades_v3_oos.csv",
        index=False,
    )

    import matplotlib.pyplot as plt

    fig, axis = plt.subplots(figsize=(10, 5))
    axis.plot(ga_oos["equity_curve"], label="GA optimizado")
    axis.plot(baseline_oos["equity_curve"], label="V3 baseline")
    axis.set_title("Curva de equity OOS (holdout final)")
    axis.set_xlabel("Trade cerrado")
    axis.set_ylabel("Equity (USD)")
    axis.grid(True, alpha=0.3)
    axis.legend()
    fig.tight_layout()
    fig.savefig(output_dir / "equity_curve.png", dpi=120)
    plt.close(fig)

    report = {
        "timestamp_utc": timestamp,
        "dataset": {
            "path": str(Path(csv_path).resolve()),
            "rows": len(data),
            "start": data["inicio"].min().isoformat(),
            "end": data["inicio"].max().isoformat(),
            "is_rows": len(is_data),
            "oos_rows": len(data) - oos_start,
            "split_fraction": 0.7,
            "oos_note": (
                "Holdout no totalmente virgen: el OOS ya se inspeccionó en "
                "comparaciones anteriores; úsese como evidencia exploratoria."
            ),
        },
        "methodology": {
            "selection": "Solo walk-forward IS; OOS no participa en la selección.",
            "folds": optimizer.folds,
            "population_size": population_size,
            "generations": generations,
            "trial_budget": optimizer.trial_budget,
            "seed": seed,
            "fixed_trend_filter": True,
            "trend_period": EMA_TREND_PERIOD,
            "search_space": {
                "ema_fast": [5, 49],
                "ema_slow": "fast + 1 through 50",
                "rsi_thresholds": [55, 60, 65, 70, 75],
                "stop_loss_pips": [5, 10, 15, 20, 25, 30],
                "take_profit_pips": list(range(5, 61, 5)),
            },
            "fitness": (
                "DSR diario IS penalizado por presupuesto de ensayos, consistencia de "
                "folds, PnL y drawdown; penaliza menos de 30 trades totales o menos "
                "de 5 en cualquier fold."
            ),
        },
        "baseline_v3": {
            "candidate": baseline.__dict__,
            "is_walk_forward": baseline_is,
            "oos": {
                key: value
                for key, value in baseline_oos.items()
                if key not in {"trades", "equity_curve"}
            },
        },
        "genetic_optimizer": {
            "candidate": ga_candidate.__dict__,
            "is_walk_forward": ga_result,
            "oos": {
                key: value
                for key, value in ga_oos.items()
                if key not in {"trades", "equity_curve"}
            },
        },
        "recommendation": (
            "Comparar el OOS de forma descriptiva; no seleccionar parámetros usando "
            "este resultado. No activar en trading real sin validación independiente."
        ),
    }
    with (output_dir / "metrics.json").open("w", encoding="utf-8") as report_file:
        json.dump(report, report_file, indent=2, allow_nan=False)

    log.info(
        "V3 OOS: PnL=%s | Sharpe=%s | trades=%s",
        baseline_oos["metrics"]["total_pnl"],
        baseline_oos["metrics"]["sharpe"],
        baseline_oos["metrics"]["num_trades"],
    )
    log.info(
        "GA OOS: PnL=%s | Sharpe=%s | trades=%s",
        ga_oos["metrics"]["total_pnl"],
        ga_oos["metrics"]["sharpe"],
        ga_oos["metrics"]["num_trades"],
    )
    log.info("Reporte: %s", output_dir)
    return output_dir


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", type=Path, default=DATA_DIR / "EURUSD_M1.csv")
    parser.add_argument("--population-size", type=int, default=20)
    parser.add_argument("--generations", type=int, default=8)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()
    run_ga_comparison(
        csv_path=args.data,
        population_size=args.population_size,
        generations=args.generations,
        seed=args.seed,
    )
