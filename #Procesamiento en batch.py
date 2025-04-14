#Procesamiento en batch

#Cargar el conjunto de datos seleccionados desde la fuente original//

start-all.sh #Preparación del entorno
	hdfs dfs -mkdir /Tarea3 #Crear una carpeta en HDFS
	wget https://www.datos.gov.co/api/views/96sh-4v8d/rows.csv #Descargar el dataset
	hdfs dfs -put /home/hadoop/rows.csv /Tarea3 #Subir el dataset a HDFS
    
#Procesamiento de datos con PySpark

	from pyspark.sql import SparkSession, functions as F #Importamos librerias necesarias
	spark = SparkSession.builder.appName('Tarea3').getOrCreate() # Inicializa la sesión de Spark
	file_path = 'hdfs://localhost:9000/Tarea3/rows.csv' # Define la ruta del archivo .csv en HDFS
	df = spark.read.format('csv').option('header','true').option('inferSchema', 'true').load(file_path) # Lee el archivo .csv
	df.printSchema() #imprimimos el esquema
	df.show() # Muestra las primeras filas del DataFrame
	df.summary().show() # Estadisticas básicas
    
# Consulta Filtrar por valor y seleccionar columnas
	print("Dias con valor mayor a 5000\n")
	dias = df.filter(F.col('VALOR') > 5000).select('VALOR','VIGENCIADESDE','VIGENCIAHASTA')
	dias.show()
# Ordenar filas por los valores en la columna VALOR en orden descendente
	print("Valores ordenados de mayor a menor\n")
	sorted_df = df.sort(F.col("VALOR").desc())
	sorted_df.show()
	
    python3 tarea3.py #Ejecución del Script 
	
#Realizar operaciones de limpieza, transformación y análisis exploratorio de datos (EDA) utilizando RDDs o DataFrames.    
    
    from pyspark.sql import SparkSession, functions as F
    from pyspark.sql.types import IntegerType

    spark = SparkSession.builder.appName('EDA_Tarea3').getOrCreate()#Iniciar sesión de Spark
    file_path = 'hdfs://localhost:9000/Tarea3/rows.csv' #Cargar el dataset desde HDFS
    df = spark.read.format('csv').option('header', 'true').option('inferSchema', 'true').load(file_path)

    df.printSchema()#Explorar la estructura del DataFrame
    df.show(5)
    
#Limpieza de datos
    df = df.withColumnRenamed('Código Iso 3166', 'Codigo_Iso') \.withColumnRenamed('Latitud - Longitud', 'Latitud_Longitud')

# Manejo de valores nulos
    df = df.fillna({'Femenino': 0, 'Masculino': 0, 'Total': 0, 'Indefinido': 'Desconocido'})

# Convertir columnas a tipos adecuados
    df = df.withColumn('Femenino', df['Femenino'].cast(IntegerType())) \.withColumn('Masculino', df['Masculino'].cast(IntegerType())) \.withColumn('Total', df['Total'].cast(IntegerType()))

#Transformaciones de datos
    df = df.withColumn('Porcentaje_Femenino', (df['Femenino'] / df['Total']) * 100) # Crear nueva columna con porcentaje de mujeres

# Filtrar valores (países con más de 50 mil visitantes)
    df_outliers = df.filter(df['Total'] > 50000)
    df_outliers.show()

#Análisis exploratorio de datos (EDA)
    df_sorted = df.orderBy(F.col("Total").desc())# Ordenar datos por cantidad de visitantes
    df_sorted.show(10)
    df.describe().show() # Estadísticas

# Convertir DataFrame a RDD y contar registros por nacionalidad
    rdd = df.rdd
    rdd_nacionalidades = rdd.map(lambda row: (row['Nacionalidad'], 1)).reduceByKey(lambda a, b: a + b)
    print(rdd_nacionalidades.collect())

# Almacenar los resultados procesados en HDFS
    df.write.csv('hdfs://localhost:9000/Tarea3/datos_limpiados.csv', header=True)
    df_outliers.write.csv('hdfs://localhost:9000/Tarea3/outliers.csv', header=True)
    df_sorted.write.csv('hdfs://localhost:9000/Tarea3/ordenados.csv', header=True)

import csv
import json
import time
from kafka import KafkaProducer

# Ruta al archivo original
csv_file_path = '/home/vboxuser/rows.csv'

# Inicializa el productor
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

with open(csv_file_path, mode='r', encoding='utf-8') as file:
    reader = csv.DictReader(file)

    for row in reader:
        # Limpieza de datos
        if 'Indefinido' in row:
            del row['Indefinido']

        # Convertir columnas numéricas eliminando comas y convirtiendo a enteros
        for col in ['Femenino', 'Masculino', 'Total']:
            row[col] = int(row[col].replace(',', '')) if row[col] else 0

        # Separa latitud y longitud
        if 'Latitud - Longitud' in row and row['Latitud - Longitud']:
            try:
                lat, lon = row['Latitud - Longitud'].split(',')
                row['Latitud'] = float(lat.strip())
                row['Longitud'] = float(lon.strip())
            except ValueError:
                row['Latitud'] = None
                row['Longitud'] = None
        
        # Eliminar la columna original 'Latitud - Longitud'
        row.pop('Latitud - Longitud', None)

        # Enviar mensaje limpio a Kafka
        producer.send('sensor_data_analisis', value=row)
        print(f"Enviado a Kafka: {row}")
        time.sleep(0.5)  # Ajusta la velocidad de envío

producer.flush()



# Importamos librerías necesarias
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
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

# Inicializa la sesión de Spark
spark = SparkSession.builder.appName('AnalisisMigracion').getOrCreate()

# Define la ruta del archivo en HDFS
file_path = 'hdfs://localhost:9000/Tarea3_practica/rows.csv'

# Carga el archivo CSV en un DataFrame de Spark
df = spark.read.format('csv').option('header', 'true').option('inferSchema', 'true').load(file_path)

# Imprimir el esquema del DataFrame
print("\n==========ESQUEMA DEL DATAFRAME==========\n" + "-"*40)
df.printSchema()

# Mostrar las primeras filas
print("\n==========MUESTRA DE DATOS (5 primeras filas)==========\n" + "-"*40)
df.show(5)

# LIMPIEZA DE DATOS
print("\n==========LIMPIEZA DE DATOS==========\n" + "-"*40)
df_clean = df.dropna(subset=['Total', 'Codigo Iso 3166'])

# Transformación: Convertir columnas numéricas a tipo entero después de limpiar comas
for col_name in ['Femenino', 'Masculino', 'Indefinido', 'Total']:
    df_clean = df_clean.withColumn(col_name, F.regexp_replace(F.col(col_name), ',', '').cast('int'))

# Verificar si la columna "Indefinido" existe y eliminarla si es necesario
if "Indefinido" in df_clean.columns:
    df_clean = df_clean.drop("Indefinido")

# Estadísticas básicas
print("\n==========ESTADÍSTICAS BÁSICAS==========\n" + "-"*40)
df_clean.summary().show()

# ANÁLISIS EXPLORATORIO (EDA)
print("\n==========ANÁLISIS EXPLORATORIO==========\n" + "-"*40)

# Contar registros por año
print("\n==========Entradas registradas por año==========\n" + "-"*40)
df_clean.groupBy("Año").count().orderBy("Año").show()

# Valores máximos y mínimos de entradas
print("\n==========Valores máximos y mínimos de entradas==========\n" + "-"*40)
df_clean.agg(F.max("Total").alias("MAX_ENTRADAS"), F.min("Total").alias("MIN_ENTRADAS")).show()

# Filtrar y mostrar registros donde el total de entradas sea mayor a 5000
print("\n==========Meses con más de 5000 entradas por año==========\n" + "-"*40)
dias_altos = df_clean.filter(F.col('Total') > 5000).select('Total', 'Año', 'Mes', 'Nacionalidad')
dias_altos.show()

# Ordenar por la cantidad de entradas en orden descendente
print("\n==========Top 10 de meses con más entradas==========\n" + "-"*40)
df_clean.orderBy(F.col("Total").desc()).show(10)

# Entradas por nacionalidad
print("\n==========Total de entradas por nacionalidad==========\n" + "-"*40)
df_clean.groupBy("Nacionalidad").agg(F.sum("Total").alias("Total Entradas")).orderBy(F.col("Total Entradas").desc()).show(10)

# Spark sigue activo para la interfaz gráfica
print("\n==========Análisis completado. Spark sigue en ejecución para visualización.==========")
