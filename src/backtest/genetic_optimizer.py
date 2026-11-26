"""Genetic search for EMA/RSI/SL/TP parameters using walk-forward IS folds."""
from dataclasses import asdict, dataclass
from math import sqrt, tanh
from random import Random
from typing import Dict, List, Tuple

import pandas as pd

from config.settings import EMA_TREND_PERIOD
from src.backtest.engine import BacktestEngine, trades_to_dicts
from src.backtest.metrics import (
    all_metrics,
    compute_daily_returns,
    deflated_sharpe,
)


FAST_MIN = 5
FAST_MAX = 49
SLOW_MAX = 50
RSI_LEVELS = (55, 60, 65, 70, 75)
SL_VALUES = (5, 10, 15, 20, 25, 30)
TP_VALUES = (5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60)
MINIMUM_VALIDATION_TRADES = 30
MINIMUM_TRADES_PER_FOLD = 5


@dataclass(frozen=True, order=True)
class Candidate:
    """One valid combination of strategy parameters tested by the search."""

    fast: int
    slow: int
    rsi_level: int
    sl_pips: int
    tp_pips: int


def walk_forward_folds(
    n_rows: int,
    fold_count: int = 3,
    initial_train_fraction: float = 0.4,
) -> List[Tuple[int, int]]:
    """Return disjoint validation windows after an initial indicator warmup."""
    if fold_count < 1:
        raise ValueError("fold_count must be at least 1.")
    if not 0 < initial_train_fraction < 1:
        raise ValueError("initial_train_fraction must be between 0 and 1.")

    first_validation = int(n_rows * initial_train_fraction)
    remaining = n_rows - first_validation
    if remaining < fold_count:
        raise ValueError("Not enough rows to create the requested walk-forward folds.")

    folds = []
    for index in range(fold_count):
        start = first_validation + remaining * index // fold_count
        end = first_validation + remaining * (index + 1) // fold_count
        if end <= start:
            raise ValueError("A walk-forward validation window is empty.")
        folds.append((start, end))
    return folds


def _equity_from_daily_returns(
    daily_returns: List[float],
    initial_capital: float,
) -> List[float]:
    equity_curve = [initial_capital]
    for daily_return in daily_returns:
        equity_curve.append(equity_curve[-1] + daily_return * initial_capital)
    return equity_curve


def _engine_for(
    candidate: Candidate,
    initial_capital: float,
    use_rsi_filter: bool,
) -> BacktestEngine:
    return BacktestEngine(
        fast=candidate.fast,
        slow=candidate.slow,
        sl_pips=candidate.sl_pips,
        tp_pips=candidate.tp_pips,
        initial_capital=initial_capital,
        use_rsi_filter=use_rsi_filter,
        use_trend_filter=True,
        rsi_overbought=float(candidate.rsi_level),
        rsi_oversold=float(100 - candidate.rsi_level),
        trend_period=EMA_TREND_PERIOD,
    )


def evaluate_window(
    data: pd.DataFrame,
    candidate: Candidate,
    start_index: int,
    n_trials: int,
    initial_capital: float = 10000.0,
    use_rsi_filter: bool = True,
) -> dict:
    """Evaluate one time range while preserving earlier rows for indicator warm-up."""
    engine = _engine_for(candidate, initial_capital, use_rsi_filter)
    trades, equity_curve = engine.run(data, start_index=start_index)
    trade_dicts = trades_to_dicts(trades)
    daily_returns = compute_daily_returns(trade_dicts, initial_capital)
    metrics = all_metrics(
        trade_dicts,
        equity_curve,
        daily_returns,
        initial_capital,
    )
    dsr = deflated_sharpe(
        metrics["sharpe"] / sqrt(252),
        n_trials=max(1, n_trials),
        n_obs=len(daily_returns),
    )
    return {
        "metrics": metrics,
        "deflated_sharpe": round(dsr, 6),
        "n_daily_observations": len(daily_returns),
        "trades": trade_dicts,
        "equity_curve": equity_curve,
    }


def evaluate_walk_forward(
    data: pd.DataFrame,
    candidate: Candidate,
    folds: List[Tuple[int, int]],
    n_trials: int,
    initial_capital: float = 10000.0,
    use_rsi_filter: bool = True,
) -> dict:
    """Evaluate only each validation window; earlier rows provide indicator history."""
    if not folds:
        raise ValueError("At least one walk-forward fold is required.")
    all_trades = []
    fold_results = []

    for fold_number, (start, end) in enumerate(folds, start=1):
        prefix = data.iloc[:end].reset_index(drop=True)
        result = evaluate_window(
            prefix,
            candidate,
            start_index=start,
            n_trials=n_trials,
            initial_capital=initial_capital,
            use_rsi_filter=use_rsi_filter,
        )
        trade_dicts = result["trades"]
        all_trades.extend(trade_dicts)
        fold_results.append({
            "fold": fold_number,
            "start_index": start,
            "end_index": end,
            "metrics": result["metrics"],
            "deflated_sharpe": result["deflated_sharpe"],
        })

    daily_returns = compute_daily_returns(all_trades, initial_capital)
    aggregate_metrics = all_metrics(
        all_trades,
        _equity_from_daily_returns(daily_returns, initial_capital),
        daily_returns,
        initial_capital,
    )
    dsr = deflated_sharpe(
        aggregate_metrics["sharpe"] / sqrt(252),
        n_trials=max(1, n_trials),
        n_obs=len(daily_returns),
    )
    positive_folds = sum(
        fold["metrics"]["total_pnl"] > 0 for fold in fold_results
    )
    consistency = positive_folds / len(fold_results)
    fitness = (
        dsr
        + 0.05 * consistency
        + 0.05 * tanh(aggregate_metrics["total_pnl"] / initial_capital)
        - 0.05 * aggregate_metrics["max_drawdown"]
    )
    if (
        aggregate_metrics["num_trades"] < MINIMUM_VALIDATION_TRADES
        or any(
            fold["metrics"]["num_trades"] < MINIMUM_TRADES_PER_FOLD
            for fold in fold_results
        )
    ):
        fitness -= 1.0

    return {
        "candidate": asdict(candidate),
        "use_rsi_filter": use_rsi_filter,
        "metrics": aggregate_metrics,
        "deflated_sharpe": round(dsr, 6),
        "fitness": round(fitness, 6),
        "positive_folds": positive_folds,
        "fold_count": len(fold_results),
        "folds": fold_results,
        "n_trials": n_trials,
        "n_daily_observations": len(daily_returns),
    }


class GeneticOptimizer:
    """Search strategy parameters with a seeded genetic algorithm.

    Candidate fitness is measured only on walk-forward validation folds.
    Rows preceding each fold initialize indicators but cannot produce trades.
    """

    def __init__(
        self,
        data: pd.DataFrame,
        population_size: int = 20,
        generations: int = 8,
        fold_count: int = 3,
        seed: int = 42,
        initial_capital: float = 10000.0,
        mutation_rate: float = 0.25,
    ):
        if population_size < 4:
            raise ValueError("population_size must be at least 4.")
        if generations < 1:
            raise ValueError("generations must be at least 1.")
        if not 0 <= mutation_rate <= 1:
            raise ValueError("mutation_rate must be between 0 and 1.")

        self.data = data.reset_index(drop=True)
        self.population_size = population_size
        self.generations = generations
        self.initial_capital = initial_capital
        self.mutation_rate = mutation_rate
        self.seed = seed
        self.rng = Random(seed)
        self.folds = walk_forward_folds(len(self.data), fold_count)
        self.trial_budget = population_size * generations
        self.cache: Dict[Candidate, dict] = {}

    @staticmethod
    def _repair(candidate: Candidate) -> Candidate:
        """Clamp and align candidate parameters to the supported search space."""
        fast = min(FAST_MAX, max(FAST_MIN, candidate.fast))
        slow = min(SLOW_MAX, max(fast + 1, candidate.slow))
        rsi_level = min(RSI_LEVELS[-1], max(RSI_LEVELS[0], candidate.rsi_level))
        rsi_level = min(RSI_LEVELS, key=lambda value: abs(value - rsi_level))
        sl_pips = min(SL_VALUES, key=lambda value: abs(value - candidate.sl_pips))
        tp_pips = min(TP_VALUES, key=lambda value: abs(value - candidate.tp_pips))
        return Candidate(fast, slow, rsi_level, sl_pips, tp_pips)

    def random_candidate(self) -> Candidate:
        """Create a valid random parameter combination."""
        fast = self.rng.randint(FAST_MIN, FAST_MAX)
        return Candidate(
            fast=fast,
            slow=self.rng.randint(fast + 1, SLOW_MAX),
            rsi_level=self.rng.choice(RSI_LEVELS),
            sl_pips=self.rng.choice(SL_VALUES),
            tp_pips=self.rng.choice(TP_VALUES),
        )

    def crossover(self, left: Candidate, right: Candidate) -> Candidate:
        """Build a child by selecting each parameter from either parent."""
        return self._repair(Candidate(
            fast=self.rng.choice((left.fast, right.fast)),
            slow=self.rng.choice((left.slow, right.slow)),
            rsi_level=self.rng.choice((left.rsi_level, right.rsi_level)),
            sl_pips=self.rng.choice((left.sl_pips, right.sl_pips)),
            tp_pips=self.rng.choice((left.tp_pips, right.tp_pips)),
        ))

    def mutate(self, candidate: Candidate) -> Candidate:
        """Randomly replace selected parameters, then restore valid constraints."""
        genes = asdict(candidate)
        if self.rng.random() < self.mutation_rate:
            genes["fast"] = self.rng.randint(FAST_MIN, FAST_MAX)
        if self.rng.random() < self.mutation_rate:
            genes["slow"] = self.rng.randint(FAST_MIN + 1, SLOW_MAX)
        if self.rng.random() < self.mutation_rate:
            genes["rsi_level"] = self.rng.choice(RSI_LEVELS)
        if self.rng.random() < self.mutation_rate:
            genes["sl_pips"] = self.rng.choice(SL_VALUES)
        if self.rng.random() < self.mutation_rate:
            genes["tp_pips"] = self.rng.choice(TP_VALUES)
        return self._repair(Candidate(**genes))

    def _score(self, candidate: Candidate) -> dict:
        """Evaluate a candidate once and cache its walk-forward result."""
        if candidate not in self.cache:
            self.cache[candidate] = evaluate_walk_forward(
                self.data,
                candidate,
                self.folds,
                n_trials=self.trial_budget,
                initial_capital=self.initial_capital,
                use_rsi_filter=True,
            )
        return self.cache[candidate]

    def evaluate_fixed(
        self,
        candidate: Candidate,
        use_rsi_filter: bool,
        n_trials: int,
    ) -> dict:
        """Evaluate a fixed configuration, such as the existing baseline."""
        return evaluate_walk_forward(
            self.data,
            candidate,
            self.folds,
            n_trials=n_trials,
            initial_capital=self.initial_capital,
            use_rsi_filter=use_rsi_filter,
        )

    def optimize(self) -> dict:
        """Run the configured generations and return the highest-fitness candidate."""
        population = {
            Candidate(
                fast=9,
                slow=21,
                rsi_level=70,
                sl_pips=10,
                tp_pips=20,
            )
        }
        while len(population) < self.population_size:
            population.add(self.random_candidate())

        history = []
        elite_count = max(1, self.population_size // 10)
        for generation in range(self.generations):
            ranked = sorted(
                (self._score(candidate) for candidate in sorted(population)),
                key=lambda result: result["fitness"],
                reverse=True,
            )
            best = ranked[0]
            history.append({
                "generation": generation + 1,
                "fitness": best["fitness"],
                "candidate": best["candidate"],
            })
            if generation == self.generations - 1:
                break

            next_population = {
                Candidate(**result["candidate"])
                for result in ranked[:elite_count]
            }
            while len(next_population) < self.population_size:
                parents = self.rng.sample(ranked, 2)
                child = self.crossover(
                    Candidate(**parents[0]["candidate"]),
                    Candidate(**parents[1]["candidate"]),
                )
                next_population.add(self.mutate(child))
            population = next_population

        best = max(
            (self._score(candidate) for candidate in sorted(population)),
            key=lambda result: result["fitness"],
        )
        return {
            **best,
            "history": history,
            "unique_candidates_evaluated": len(self.cache),
            "trial_budget": self.trial_budget,
            "seed": self.seed,
        }
