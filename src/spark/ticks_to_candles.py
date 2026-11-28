"""Transforma ticks de precios de Kafka en mensajes OHLC por intervalo de tiempo."""
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
    """Convierte los ticks de precios en una vela OHLC por intervalo.

    El precio de la vela es el punto medio entre bid y ask:

    * ``open``: primer punto medio del intervalo.
    * ``high`` y ``low``: puntos medios máximo y mínimo.
    * ``close``: último punto medio del intervalo.

    La transformación no depende de Kafka ni de IB, por lo que las pruebas
    pueden usar datos sintéticos. Los campos ``inicio`` y ``num_ticks`` se
    conservan porque forman parte del contrato de mensajes Kafka existente.
    """
    is_streaming = ticks_df.isStreaming

    clean_ts = F.regexp_replace(F.col("timestamp"), "Z$", "")
    parsed = ticks_df.withColumn(
        "event_time",
        F.coalesce(
            F.try_to_timestamp(clean_ts, F.lit("yyyy-MM-dd'T'HH:mm:ss.SSSSSS")),
            F.try_to_timestamp(clean_ts, F.lit("yyyy-MM-dd'T'HH:mm:ss.SSS")),
            F.try_to_timestamp(clean_ts, F.lit("yyyy-MM-dd'T'HH:mm:ss")),
            F.try_to_timestamp(F.col("timestamp")),
        ),
    )
    parsed = parsed.withColumn("mid", (F.col("bid") + F.col("ask")) / 2)

    # Los marcadores de agua solo se admiten en DataFrames de streaming.
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
    """Crea una sesión Spark local con el conector Kafka y la zona horaria UTC."""
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
    """Ejecuta el proceso que lee ticks y publica las velas cerradas."""
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

    # Conserva el directorio para que el flujo reanude sus offsets anteriores.
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