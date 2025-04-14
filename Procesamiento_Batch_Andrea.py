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