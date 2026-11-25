from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

from config.settings import (
    KAFKA_BOOTSTRAP_SERVERS, TOPIC_TICKS, TOPIC_VELAS,
    WINDOW_SECONDS, BASE_DIR,
)
from src.common.logger import get_logger

log = get_logger("ticks_to_velas")

TICK_SCHEMA = StructType([
    StructField("symbol", StringType(), True),
    StructField("bid", DoubleType(), True),
    StructField("ask", DoubleType(), True),
    StructField("timestamp", StringType(), True),
])


def transform(ticks_df: DataFrame) -> DataFrame:
    """
    Convierte un DataFrame de ticks a velas OHLC.

    - open  = mid del primer tick de la ventana
    - high  = max(mid) de la ventana
    - low   = min(mid) de la ventana
    - close = mid del último tick de la ventana
    - mid   = (bid + ask) / 2

    Es puro (sin Kafka, sin IB), lo que permite testearlo con datos sintéticos.
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

    # Watermark solo aplica en streaming; en batch se omite
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
    """Crea la SparkSession con el conector Kafka y timezone UTC."""
    return (
        SparkSession.builder
        .appName("TicksToVelas")
        .master("local[*]")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.1",
        )
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def run():
    """Job de streaming: lee ticks, transforma, escribe velas."""
    from src.common.kafka_utils import create_topic_if_not_exists
    create_topic_if_not_exists(TOPIC_VELAS)

    spark = get_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    log.info(f"SparkSession creada (v{spark.version}).")

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

    velas = transform(parsed)

    output = velas.select(
        F.to_json(F.struct(
            "symbol", "inicio", "open", "high", "low", "close", "num_ticks",
        )).alias("value")
    )

    checkpoint = str(BASE_DIR / "checkpoints" / "velas")

    query = (
        output.writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("topic", TOPIC_VELAS)
        .option("checkpointLocation", checkpoint)
        .outputMode("append")
        .start()
    )

    log.info(f"Streaming iniciado. Checkpoint: {checkpoint}")
    log.info(f"Escuchando '{TOPIC_TICKS}' y publicando velas cada {WINDOW_SECONDS}s. Ctrl+C para detener.")

    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        log.info("Detenido por el usuario.")
    finally:
        query.stop()
        spark.stop()
        log.info("Spark detenido.")