"""Offline tests for EMA indicators and strategy signal generation.

These tests do not require Kafka or Interactive Brokers.
"""
from src.algorithms.indicadores import ema_series, ema_last, detect_cross
from src.algorithms.ema_cross import EMACross


def test_ema_series_known_values():
    """Check EMA(3) over [1, 2, 3, 4, 5] with an SMA seed."""
    values = [1.0, 2.0, 3.0, 4.0, 5.0]
    ema = ema_series(values, 3)
    # Warm-up: indices 0 and 1 are None.
    assert ema[0] is None and ema[1] is None
    # SMA seed at index 2 = (1 + 2 + 3) / 3 = 2.0.
    assert abs(ema[2] - 2.0) < 1e-9, f"seed={ema[2]}"
    # alpha = 2/(3+1) = 0.5
    # ema[3] = 0.5*4 + 0.5*2 = 3.0
    assert abs(ema[3] - 3.0) < 1e-9, f"ema[3]={ema[3]}"
    # ema[4] = 0.5*5 + 0.5*3 = 4.0
    assert abs(ema[4] - 4.0) < 1e-9, f"ema[4]={ema[4]}"
    print("PASS test_ema_series_known_values")


def test_ema_insufficient_data():
    assert ema_series([1.0, 2.0], 5) == [None, None]
    assert ema_last([1.0, 2.0], 5) is None
    print("PASS test_ema_insufficient_data")


def test_detect_cross_buy():
    # Before: fast=1.079, slow=1.080 (fast < slow).
    # After: fast=1.082, slow=1.0805 (fast > slow), producing BUY.
    assert detect_cross(1.079, 1.080, 1.082, 1.0805) == "BUY"
    print("PASS test_detect_cross_buy")


def test_detect_cross_sell():
    # Before: fast=1.082, slow=1.0805 (fast > slow).
    # After: fast=1.078, slow=1.0805 (fast < slow), producing SELL.
    assert detect_cross(1.082, 1.0805, 1.078, 1.0805) == "SELL"
    print("PASS test_detect_cross_sell")


def test_detect_cross_none():
    # No crossover.
    assert detect_cross(1.079, 1.080, 1.0785, 1.0795) is None
    # Insufficient data.
    assert detect_cross(None, 1.080, 1.082, 1.0805) is None
    print("PASS test_detect_cross_none")


def _make_vela(close: float) -> dict:
    return {
        "symbol": "EURUSD",
        "inicio": "2026-01-01T12:00:00.000Z",
        "open": close, "high": close, "low": close, "close": close,
        "num_ticks": 100,
    }


def test_ema_cross_buy_signal():
    """A gentle decline followed by a sharp rise must produce BUY."""
    algo = EMACross(fast=9, slow=21)

    # Start with 25 flat candles at 1.0800.
    prices = [1.0800] * 25
    # Three gently falling candles make fast EMA lower than slow EMA.
    prices += [1.0795, 1.0790, 1.0785]
    # Three sharply rising candles make fast EMA cross above slow EMA.
    prices += [1.0820, 1.0850, 1.0880]

    result = None
    for price in prices:
        r = algo.process_vela(_make_vela(price))
        if r is not None:
            result = r
    assert result is not None, "Expected at least one BUY signal."
    assert result["signal"] == "BUY", f"Signal={result['signal']}, expected BUY."
    print(f"PASS test_ema_cross_buy_signal (BUY at {result['precio_referencia']:.5f})")


def test_ema_cross_sell_signal():
    """A gentle rise followed by a sharp decline must produce SELL."""
    algo = EMACross(fast=9, slow=21)

    prices = [1.0800] * 25
    prices += [1.0805, 1.0810, 1.0815]  # A gentle rise makes fast EMA exceed slow EMA.
    prices += [1.0780, 1.0750, 1.0720]  # A sharp decline makes fast EMA cross below slow EMA.

    result = None
    for price in prices:
        r = algo.process_vela(_make_vela(price))
        if r is not None:
            result = r
    assert result is not None, "Expected at least one SELL signal."
    assert result["signal"] == "SELL", f"Signal={result['signal']}, expected SELL."
    print(f"PASS test_ema_cross_sell_signal (SELL at {result['precio_referencia']:.5f})")


def test_no_duplicate_signals():
    """A flat-to-down-to-up sequence must emit exactly one BUY."""
    algo = EMACross(fast=9, slow=21)

    prices = [1.0800] * 25
    prices += [1.0795, 1.0790, 1.0785]
    prices += [1.0820, 1.0850, 1.0880]

    signals = []
    for price in prices:
        r = algo.process_vela(_make_vela(price))
        if r is not None:
            signals.append(r["signal"])

    assert signals == ["SELL", "BUY"], f"Sequence={signals}, expected [SELL, BUY]."
    assert signals.count("BUY") == 1
    print(f"PASS test_no_duplicate_signals (sequence: {signals})")


def test_no_signal_during_warmup():
    """Fewer than `slow + 1` candles must not produce a signal."""
    algo = EMACross(fast=9, slow=21)
    for price in [1.0800] * 15:  # 15 < 22
        r = algo.process_vela(_make_vela(price))
        assert r is None
    print("PASS test_no_signal_during_warmup")


if __name__ == "__main__":
    print("Phase 3 unit tests")
    test_ema_series_known_values()
    test_ema_insufficient_data()
    test_detect_cross_buy()
    test_detect_cross_sell()
    test_detect_cross_none()
    test_ema_cross_buy_signal()
    test_ema_cross_sell_signal()
    test_no_duplicate_signals()
    test_no_signal_during_warmup()
    print("All Phase 3 tests passed.")