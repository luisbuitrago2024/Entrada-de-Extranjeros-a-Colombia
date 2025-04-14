    from pyspark.sql import SparkSession
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType
    from pyspark.sql.functions import col

#Configurar Spark
    spark = SparkSession.builder.appName("KafkaSparkStreaming").getOrCreate()

#Configurar Kafka
    kafka_topic = "ingresos_extranjeros"
    kafka_bootstrap_servers = "localhost:9092"

#Definir el esquema de los datos recibidos
    schema = StructType([
        StructField("Año", StringType(), True),
        StructField("Mes", StringType(), True),
        StructField("Nacionalidad", StringType(), True),
        StructField("Codigo_Iso", StringType(), True),
        StructField("Femenino", IntegerType(), True),
        StructField("Masculino", IntegerType(), True),
        StructField("Total", IntegerType(), True)
    ])

#Leer datos en tiempo real desde Kafka
    streaming_df = spark.readStream.format("kafka") \.option("kafka.bootstrap.servers", kafka_bootstrap_servers) \.option("subscribe", kafka_topic) \
    .option("startingOffsets", "latest") \.load()

#Convertir los datos de Kafka a DataFrame
    streaming_df = streaming_df.selectExpr("CAST(value AS STRING)")

#Separar los datos en columnas
    streaming_df = streaming_df.selectExpr(
        "split(value, ',')[0] as Año",
        "split(value, ',')[1] as Mes",
        "split(value, ',')[2] as Nacionalidad",
        "split(value, ',')[3] as Codigo_Iso",
        "split(value, ',')[4] as Femenino",
        "split(value, ',')[5] as Masculino",
        "split(value, ',')[6] as Total"
    ).withColumn("Femenino", col("Femenino").cast(IntegerType())) \.withColumn("Masculino", col("Masculino").cast(IntegerType())) \.withColumn("Total", col("Total").cast(IntegerType()))

#Procesamiento en tiempo real (contar ingresos por nacionalidad)
    conteo_nacionalidad = streaming_df.groupBy("Nacionalidad").count()

#Visualizar resultados en consola
    query = conteo_nacionalidad.writeStream \.outputMode("complete") \.format("console") \.start()
    query.awaitTermination()
