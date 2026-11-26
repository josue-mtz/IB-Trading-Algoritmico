"""Pruebas locales de indicadores EMA y generación de señales de estrategia.

Estas pruebas no requieren Kafka ni Interactive Brokers.
"""
from src.algorithms.indicadores import ema_series, ema_last, detect_cross
from src.algorithms.ema_cross import EMACross


def test_ema_series_known_values():
    """Comprueba EMA(3) sobre [1, 2, 3, 4, 5] con una semilla SMA."""
    values = [1.0, 2.0, 3.0, 4.0, 5.0]
    ema = ema_series(values, 3)
    # Durante el calentamiento, los índices 0 y 1 son None.
    assert ema[0] is None and ema[1] is None
    # La semilla SMA en el índice 2 es (1 + 2 + 3) / 3 = 2.0.
    assert abs(ema[2] - 2.0) < 1e-9, f"seed={ema[2]}"
    # alpha = 2/(3+1) = 0.5.
    # ema[3] = 0.5*4 + 0.5*2 = 3.0.
    assert abs(ema[3] - 3.0) < 1e-9, f"ema[3]={ema[3]}"
    # ema[4] = 0.5*5 + 0.5*3 = 4.0.
    assert abs(ema[4] - 4.0) < 1e-9, f"ema[4]={ema[4]}"
    print("PASS test_ema_series_known_values")


def test_ema_insufficient_data():
    assert ema_series([1.0, 2.0], 5) == [None, None]
    assert ema_last([1.0, 2.0], 5) is None
    print("PASS test_ema_insufficient_data")


def test_detect_cross_buy():
    # Antes: fast=1.079, slow=1.080 (fast < slow).
    # Después: fast=1.082, slow=1.0805 (fast > slow), genera BUY.
    assert detect_cross(1.079, 1.080, 1.082, 1.0805) == "BUY"
    print("PASS test_detect_cross_buy")


def test_detect_cross_sell():
    # Antes: fast=1.082, slow=1.0805 (fast > slow).
    # Después: fast=1.078, slow=1.0805 (fast < slow), genera SELL.
    assert detect_cross(1.082, 1.0805, 1.078, 1.0805) == "SELL"
    print("PASS test_detect_cross_sell")


def test_detect_cross_none():
    # No hay cruce.
    assert detect_cross(1.079, 1.080, 1.0785, 1.0795) is None
    # Datos insuficientes.
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
    """Una caída leve seguida de una subida fuerte debe producir BUY."""
    algo = EMACross(fast=9, slow=21)

    # Inicia con 25 velas planas en 1.0800.
    prices = [1.0800] * 25
    # Tres velas con una caída leve hacen que la EMA rápida quede por debajo de la lenta.
    prices += [1.0795, 1.0790, 1.0785]
    # Tres velas con una subida fuerte hacen que la EMA rápida cruce por encima de la lenta.
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
    """Una subida leve seguida de una caída fuerte debe producir SELL."""
    algo = EMACross(fast=9, slow=21)

    prices = [1.0800] * 25
    prices += [1.0805, 1.0810, 1.0815]  # La subida leve deja la EMA rápida por encima de la lenta.
    prices += [1.0780, 1.0750, 1.0720]  # La caída fuerte hace que la EMA rápida cruce por debajo de la lenta.

    result = None
    for price in prices:
        r = algo.process_vela(_make_vela(price))
        if r is not None:
            result = r
    assert result is not None, "Expected at least one SELL signal."
    assert result["signal"] == "SELL", f"Signal={result['signal']}, expected SELL."
    print(f"PASS test_ema_cross_sell_signal (SELL at {result['precio_referencia']:.5f})")


def test_no_duplicate_signals():
    """Una secuencia plana, bajista y alcista debe emitir exactamente un BUY."""
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
    """Con menos de `slow + 1` velas no se debe generar una señal."""
    algo = EMACross(fast=9, slow=21)
    for price in [1.0800] * 15:  # 15 es menor que 22.
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