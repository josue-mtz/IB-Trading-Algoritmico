from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("TestSpark") \
    .getOrCreate()

print(f"✅ Spark versión {spark.version} funcionando correctamente.")
spark.stop()