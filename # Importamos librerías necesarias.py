# Importamos librerías necesarias
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

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