"""Transform live Kafka price ticks into fixed-time OHLC candle messages."""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

from config.settings import (
    KAFKA_BOOTSTRAP_SERVERS, TOPIC_TICKS, TOPIC_CANDLES,
    WINDOW_SECONDS, BASE_DIR,
)
from src.common.logger import get_logger

log = get_logger("ticks_to_candles")

TICK_SCHEMA = StructType([
    StructField("symbol", StringType(), True),
    StructField("bid", DoubleType(), True),
    StructField("ask", DoubleType(), True),
    StructField("timestamp", StringType(), True),
])


def transform(ticks_df: DataFrame) -> DataFrame:
    """Convert price ticks into one OHLC candle per time window.

    The candle price is the midpoint between bid and ask:

    * ``open``: first midpoint in the window.
    * ``high`` and ``low``: highest and lowest midpoints.
    * ``close``: last midpoint in the window.

    The transformation is independent of Kafka and IB, so tests can use
    synthetic data. Output field names ``inicio`` and ``num_ticks`` remain
    unchanged because they are part of the existing Kafka message contract.
    """
    is_streaming = ticks_df.isStreaming

    parsed = ticks_df.withColumn(
        "event_time",
        F.to_timestamp(
            F.regexp_replace(F.col("timestamp"), "Z$", ""),
            "yyyy-MM-dd'T'HH:mm:ss.SSSSSS",
        ),
    )
    parsed = parsed.withColumn("mid", (F.col("bid") + F.col("ask")) / 2)

    # Watermarks are only supported for streaming DataFrames.
    if is_streaming:
        parsed = parsed.withWatermark("event_time", "10 seconds")

    window_duration = f"{WINDOW_SECONDS} seconds"

    grouped = (
        parsed
        .groupBy(
            F.col("symbol"),
            F.window(F.col("event_time"), window_duration).alias("w"),
        )
        .agg(
            F.expr("min_by(mid, event_time)").alias("open"),
            F.max("mid").alias("high"),
            F.min("mid").alias("low"),
            F.expr("max_by(mid, event_time)").alias("close"),
            F.count("*").alias("num_ticks"),
        )
    )

    return grouped.select(
        F.col("symbol"),
        F.col("w.start").alias("inicio"),
        F.col("open"),
        F.col("high"),
        F.col("low"),
        F.col("close"),
        F.col("num_ticks"),
    )


def get_spark_session() -> SparkSession:
    """Create the local Spark session with the Kafka connector and UTC time zone."""
    return (
        SparkSession.builder
        .appName("TicksToCandles")
        .master("local[*]")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.1",
        )
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def run():
    """Run the streaming job that reads ticks and publishes closed candles."""
    from src.common.kafka_utils import create_topic_if_not_exists
    create_topic_if_not_exists(TOPIC_CANDLES)

    spark = get_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    log.info("Spark session created (version %s).", spark.version)

    ticks_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", TOPIC_TICKS)
        .option("startingOffsets", "latest")
        .load()
    )

    parsed = (
        ticks_stream
        .select(F.from_json(F.col("value").cast("string"), TICK_SCHEMA).alias("t"))
        .select("t.*")
    )

    candles = transform(parsed)

    output = candles.select(
        F.to_json(F.struct(
            "symbol", "inicio", "open", "high", "low", "close", "num_ticks",
        )).alias("value")
    )

    # Keep the existing directory so an existing stream can resume its offsets.
    checkpoint = str(BASE_DIR / "checkpoints" / "velas")

    query = (
        output.writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("topic", TOPIC_CANDLES)
        .option("checkpointLocation", checkpoint)
        .outputMode("append")
        .start()
    )

    log.info("Streaming started. Checkpoint: %s", checkpoint)
    log.info(
        "Reading '%s' and publishing candles every %s seconds. Press Ctrl+C to stop.",
        TOPIC_TICKS,
        WINDOW_SECONDS,
    )

    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        log.info("Stopped by user.")
    finally:
        query.stop()
        spark.stop()
        log.info("Spark stopped.")