"""Pruebas unitarias locales del optimizador genético walk-forward."""
import math

import pandas as pd
import pytest

from src.backtest.genetic_optimizer import (
    Candidate,
    GeneticOptimizer,
    evaluate_window,
    walk_forward_folds,
)


def _synthetic_data(rows=300):
    closes = [
        1.1 + 0.0015 * math.sin(index / 4) + 0.0002 * math.sin(index / 15)
        for index in range(rows)
    ]
    return pd.DataFrame({
        "symbol": ["EURUSD"] * rows,
        "inicio": pd.date_range("2026-01-01", periods=rows, freq="min", tz="UTC"),
        "open": closes,
        "high": [price + 0.00015 for price in closes],
        "low": [price - 0.00015 for price in closes],
        "close": closes,
        "num_ticks": [10] * rows,
    })


def test_walk_forward_folds_are_contiguous_and_disjoint():
    assert walk_forward_folds(100, fold_count=3, initial_train_fraction=0.4) == [
        (40, 60),
        (60, 80),
        (80, 100),
    ]
    with pytest.raises(ValueError):
        walk_forward_folds(2, fold_count=3)


def test_candidate_genes_stay_inside_search_space():
    optimizer = GeneticOptimizer(
        _synthetic_data(),
        population_size=4,
        generations=1,
        seed=17,
    )
    left = Candidate(5, 6, 55, 5, 5)
    right = Candidate(49, 50, 75, 30, 60)
    for _ in range(50):
        candidate = optimizer.mutate(optimizer.crossover(left, right))
        assert 5 <= candidate.fast < candidate.slow <= 50
        assert candidate.rsi_level in (55, 60, 65, 70, 75)
        assert candidate.sl_pips in (5, 10, 15, 20, 25, 30)
        assert 5 <= candidate.tp_pips <= 60
        assert candidate.tp_pips % 5 == 0


def test_window_excludes_trades_before_validation_start():
    data = _synthetic_data()
    result = evaluate_window(
        data,
        Candidate(9, 21, 70, 10, 20),
        start_index=100,
        n_trials=10,
        use_rsi_filter=False,
    )
    assert result["trades"], "The synthetic sequence must generate trades."
    assert all(trade["entry_idx"] >= 100 for trade in result["trades"])


def test_optimizer_is_reproducible_with_fixed_seed():
    data = _synthetic_data(240)
    kwargs = {
        "data": data,
        "population_size": 4,
        "generations": 2,
        "fold_count": 3,
        "seed": 123,
    }
    first = GeneticOptimizer(**kwargs).optimize()
    second = GeneticOptimizer(**kwargs).optimize()
    assert first["candidate"] == second["candidate"]
    assert first["history"] == second["history"]
    assert first["seed"] == 123
"""Pruebas unitarias locales del optimizador genético walk-forward."""
