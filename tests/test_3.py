"""
Tests unitarios de Fase 3. NO requieren Kafka ni IB.
Solo prueban la lógica pura (indicadores + generación de señales).
"""
from src.algorithms.indicadores import ema_series, ema_last, detect_cross
from src.algorithms.ema_cross import EMACross


def test_ema_series_known_values():
    """EMA(3) sobre [1,2,3,4,5] con SMA como semilla."""
    values = [1.0, 2.0, 3.0, 4.0, 5.0]
    ema = ema_series(values, 3)
    # Warmup: índices 0 y 1 son None
    assert ema[0] is None and ema[1] is None
    # Seed SMA en índice 2 = (1+2+3)/3 = 2.0
    assert abs(ema[2] - 2.0) < 1e-9, f"seed={ema[2]}"
    # alpha = 2/(3+1) = 0.5
    # ema[3] = 0.5*4 + 0.5*2 = 3.0
    assert abs(ema[3] - 3.0) < 1e-9, f"ema[3]={ema[3]}"
    # ema[4] = 0.5*5 + 0.5*3 = 4.0
    assert abs(ema[4] - 4.0) < 1e-9, f"ema[4]={ema[4]}"
    print("✅ test_ema_series_known_values")


def test_ema_insufficient_data():
    assert ema_series([1.0, 2.0], 5) == [None, None]
    assert ema_last([1.0, 2.0], 5) is None
    print("✅ test_ema_insufficient_data")


def test_detect_cross_buy():
    # antes: fast=1.079, slow=1.080 (fast < slow)
    # ahora: fast=1.082, slow=1.0805 (fast > slow) → BUY
    assert detect_cross(1.079, 1.080, 1.082, 1.0805) == "BUY"
    print("✅ test_detect_cross_buy")


def test_detect_cross_sell():
    # antes: fast=1.082, slow=1.0805 (fast > slow)
    # ahora: fast=1.078, slow=1.0805 (fast < slow) → SELL
    assert detect_cross(1.082, 1.0805, 1.078, 1.0805) == "SELL"
    print("✅ test_detect_cross_sell")


def test_detect_cross_none():
    # sin cruce
    assert detect_cross(1.079, 1.080, 1.0785, 1.0795) is None
    # datos insuficientes
    assert detect_cross(None, 1.080, 1.082, 1.0805) is None
    print("✅ test_detect_cross_none")


def _make_vela(close: float) -> dict:
    return {
        "symbol": "EURUSD",
        "inicio": "2026-01-01T12:00:00.000Z",
        "open": close, "high": close, "low": close, "close": close,
        "num_ticks": 100,
    }


def test_ema_cross_buy_signal():
    """Bajada suave + subida brusca debe producir BUY."""
    algo = EMACross(fast=9, slow=21)

    # 25 velas planas en 1.0800
    prices = [1.0800] * 25
    # 3 velas bajando suave → fast < slow
    prices += [1.0795, 1.0790, 1.0785]
    # 3 velas subiendo fuerte → fast cruza arriba de slow
    prices += [1.0820, 1.0850, 1.0880]

    result = None
    for price in prices:
        r = algo.process_vela(_make_vela(price))
        if r is not None:
            result = r
    assert result is not None, "❌ Se esperaba al menos una señal BUY"
    assert result["signal"] == "BUY", f"❌ Señal={result['signal']}, esperado BUY"
    print(f"✅ test_ema_cross_buy_signal (BUY a {result['precio_referencia']:.5f})")


def test_ema_cross_sell_signal():
    """Subida suave + bajada brusca debe producir SELL."""
    algo = EMACross(fast=9, slow=21)

    prices = [1.0800] * 25
    prices += [1.0805, 1.0810, 1.0815]  # subida leve → fast > slow
    prices += [1.0780, 1.0750, 1.0720]  # bajada brusca → fast cruza abajo

    result = None
    for price in prices:
        r = algo.process_vela(_make_vela(price))
        if r is not None:
            result = r
    assert result is not None, "❌ Se esperaba al menos una señal SELL"
    assert result["signal"] == "SELL", f"❌ Señal={result['signal']}, esperado SELL"
    print(f"✅ test_ema_cross_sell_signal (SELL a {result['precio_referencia']:.5f})")


def test_no_duplicate_signals():
    """Flat → down → up debe emitir exactamente un BUY."""
    algo = EMACross(fast=9, slow=21)

    prices = [1.0800] * 25
    prices += [1.0795, 1.0790, 1.0785]
    prices += [1.0820, 1.0850, 1.0880]

    signals = []
    for price in prices:
        r = algo.process_vela(_make_vela(price))
        if r is not None:
            signals.append(r["signal"])

    assert signals == ["SELL", "BUY"], f"❌ Secuencia={signals}, esperado [SELL,BUY]"
    assert signals.count("BUY") == 1
    print(f"✅ test_no_duplicate_signals (secuencia: {signals})")


def test_no_signal_during_warmup():
    """Con menos de `slow + 1` velas no debe haber señal."""
    algo = EMACross(fast=9, slow=21)
    for price in [1.0800] * 15:  # 15 < 22
        r = algo.process_vela(_make_vela(price))
        assert r is None
    print("✅ test_no_signal_during_warmup")


if __name__ == "__main__":
    print(" Tests de Fase 3 ")
    test_ema_series_known_values()
    test_ema_insufficient_data()
    test_detect_cross_buy()
    test_detect_cross_sell()
    test_detect_cross_none()
    test_ema_cross_buy_signal()
    test_ema_cross_sell_signal()
    test_no_duplicate_signals()
    test_no_signal_during_warmup()
    print(" Todos los tests de Fase 3 pasaron ")