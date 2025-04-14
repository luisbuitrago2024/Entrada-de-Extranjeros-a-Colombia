from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, sum
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import time

# Crear una sesión de Spark
spark = SparkSession.builder \
    .appName("EntradasExtranjerosConsumer") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Definir el esquema de los datos que envía el producer desde el CSV
schema = StructType([
    StructField("Año", IntegerType()),
    StructField("Mes", StringType()),
    StructField("Nacionalidad", StringType()),
    StructField("Codigo Iso 3166", StringType()),
    StructField("Femenino", IntegerType()),
    StructField("Masculino", IntegerType()),
    StructField("Total", IntegerType()),
    StructField("Latitud", StringType()),  # Puede ser Null
    StructField("Longitud", StringType())  # Puede ser Null
])

# Leer datos desde Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sensor_data_analisis") \
    .load()

# Convertir el valor de Kafka de JSON string a columnas reales
parsed_df = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

# Agrupar datos por país de residencia y sumar la cantidad total de visitantes
resultado = parsed_df.groupBy("Nacionalidad") \
     .agg(sum("Total").alias("total_visitantes"))

# Guardar en memoria para consultas SQL
query = resultado.writeStream \
    .outputMode("complete") \
    .format("memory") \
    .queryName("top_paises") \
    .start()

# Bucle para ejecutar consultas periódicamente
while query.isActive:
    time.sleep(5)  # Esperar un poco para que se acumulen datos
    print("===== TOP 20 PAISES CON MÁS VISITANTES AL PAIS DE COLOMBIA=====")
    spark.sql("""
        SELECT Nacionalidad, total_visitantes 
        FROM top_paises 
        ORDER BY total_visitantes DESC 
        LIMIT 20
    """).show(truncate=False)
