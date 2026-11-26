"""Unit tests for converting synthetic ticks into OHLC candles with Spark."""
from pyspark.sql import SparkSession
from src.spark.ticks_to_velas import transform


def get_spark():
    return (
        SparkSession.builder
        .master("local[1]")
        .appName("test_fase2")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def test_spark_session():
    spark = get_spark()
    spark.sparkContext.setLogLevel("ERROR")
    print(f"PASS SparkSession created (v{spark.version}).")
    spark.stop()


def test_vela_aggregation():
    spark = get_spark()
    spark.sparkContext.setLogLevel("ERROR")

    # Three ticks in the same one-minute window (12:00:00 through 12:01:00).
    # Mid-prices: 1.08501, 1.08511, 1.08491.
    data = [
        ("EURUSD", 1.08500, 1.08502, "2025-01-01T12:00:05.000000Z"),
        ("EURUSD", 1.08510, 1.08512, "2025-01-01T12:00:30.000000Z"),
        ("EURUSD", 1.08490, 1.08492, "2025-01-01T12:00:55.000000Z"),
    ]
    df = spark.createDataFrame(data, ["symbol", "bid", "ask", "timestamp"])
    velas = transform(df).collect()

    assert len(velas) == 1, f"Expected one candle, got {len(velas)}."
    v = velas[0]

    assert abs(v["open"]  - 1.08501) < 1e-5, f"open={v['open']} != 1.08501"
    assert abs(v["high"]  - 1.08511) < 1e-5, f"high={v['high']} != 1.08511"
    assert abs(v["low"]   - 1.08491) < 1e-5, f"low={v['low']} != 1.08491"
    assert abs(v["close"] - 1.08491) < 1e-5, f"close={v['close']} != 1.08491"
    assert v["num_ticks"] == 3, f"num_ticks={v['num_ticks']} != 3"
    assert v["symbol"] == "EURUSD"
    expected_fields = {"symbol", "inicio", "open", "high", "low", "close", "num_ticks"}
    assert set(v.asDict()) == expected_fields, f"Unexpected fields: {set(v.asDict())}"
    assert v["low"] <= v["open"] <= v["high"]
    assert v["low"] <= v["close"] <= v["high"]

    print(f"PASS Candle values: open={v['open']}, high={v['high']}, low={v['low']}, close={v['close']}, num_ticks={v['num_ticks']}")
    spark.stop()


def test_multiple_windows():
    spark = get_spark()
    spark.sparkContext.setLogLevel("ERROR")

    # Two ticks in the 12:00 window and two in the 12:01 window.
    data = [
        ("EURUSD", 1.08500, 1.08502, "2025-01-01T12:00:05.000000Z"),
        ("EURUSD", 1.08510, 1.08512, "2025-01-01T12:00:30.000000Z"),
        ("EURUSD", 1.08520, 1.08522, "2025-01-01T12:01:05.000000Z"),
        ("EURUSD", 1.08480, 1.08482, "2025-01-01T12:01:55.000000Z"),
    ]
    df = spark.createDataFrame(data, ["symbol", "bid", "ask", "timestamp"])
    velas = sorted(transform(df).collect(), key=lambda v: v["inicio"])

    assert len(velas) == 2, f"Expected two candles, got {len(velas)}."
    expected_fields = {"symbol", "inicio", "open", "high", "low", "close", "num_ticks"}
    for vela in velas:
        assert set(vela.asDict()) == expected_fields
        assert vela["low"] <= vela["open"] <= vela["high"]
        assert vela["low"] <= vela["close"] <= vela["high"]
    assert velas[0]["num_ticks"] == 2
    assert velas[1]["num_ticks"] == 2
    print(f"PASS Two candles ({velas[0]['num_ticks']} ticks and {velas[1]['num_ticks']} ticks).")
    spark.stop()


if __name__ == "__main__":
    print("Phase 2 unit tests")
    test_spark_session()
    test_vela_aggregation()
    test_multiple_windows()
    print("All Phase 2 tests passed.")