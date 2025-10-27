# accidentes_batch.py
# Procesamiento Batch de Accidentes de Tránsito con Apache Spark

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, hour

# Crear sesión de Spark
spark = SparkSession.builder.appName("AccidentesBatch").getOrCreate()

# Cargar los datos (reemplaza la ruta por la tuya)
df = spark.read.csv("data/accidents.csv", header=True, inferSchema=True)

# Eliminar valores nulos
df = df.na.drop()

# Extraer la hora del accidente
df = df.withColumn("hour", hour(col("Start_Time")))

# Agrupar y contar accidentes por hora
accidents_by_hour = df.groupBy("hour").agg(count("*").alias("total_accidents"))

# Mostrar resultados
print("=== Accidentes por hora del día ===")
accidents_by_hour.orderBy("hour").show()

# Guardar resultados en un archivo CSV
accidents_by_hour.coalesce(1).write.mode("overwrite").csv("output/accidents_by_hour")

spark.stop()
