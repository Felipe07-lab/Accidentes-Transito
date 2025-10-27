# accidentes_streaming.py
# Procesamiento en Tiempo Real con Spark Streaming y Kafka

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, count
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

# Crear sesión de Spark
spark = SparkSession.builder.appName("AccidentesStreaming").getOrCreate()

# Esquema del mensaje JSON recibido desde Kafka
schema = StructType([
    StructField("Start_Time", TimestampType(), True),
    StructField("City", StringType(), True)
])

# Leer datos en tiempo real del topic "accidents_topic"
stream_df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "accidents_topic") \
    .load()

# Transformar mensajes a formato JSON legible
json_df = stream_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")).select("data.*")

# Contar accidentes por ciudad cada 5 minutos
accidents_window = json_df.groupBy(
    window(col("Start_Time"), "5 minutes"), col("City")
).agg(count("*").alias("total_accidents"))

# Mostrar resultados en consola en tiempo real
query = accidents_window.writeStream.outputMode("complete").format("console").start()
query.awaitTermination()
