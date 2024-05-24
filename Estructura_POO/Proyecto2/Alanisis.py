from pyspark.sql import SparkSession

# Crear una sesión de Spark
spark = SparkSession.builder \
    .appName("Análisis de Enfermedades Cardíacas") \
    .getOrCreate()

# Cargar el dataframe desde un archivo CSV (reemplaza 'path/to/data.csv' con la ruta real)
df = spark.read.csv('PREDICCION.csv', header=True, inferSchema=True)

    # 1. Relación entre presión arterial en reposo y frecuencia cardíaca máxima
correlation = df.corr('trestbps', 'thalach')
print("Correlación entre trestbps y thalach:", correlation)

# 2. Diferencia de edad entre pacientes con y sin angina inducida por el ejercicio
age_difference = df.groupBy('exang').agg({'age': 'avg'})
print("Diferencia de edad entre pacientes con y sin angina inducida por el ejercicio:")
age_difference.show()

# 3. Distribución de la cantidad de depresión del segmento ST inducida por el ejercicio
oldpeak_distribution = df.groupBy('oldpeak').count()
print("Distribución de oldpeak:")
oldpeak_distribution.show()

# 4. Relación entre la pendiente del segmento ST durante el ejercicio y el resultado de la prueba
slope_result = df.groupBy('slope', 'Result').count()
print("Relación entre slope y Result:")
slope_result.show()

# 5. Distribución de resultados de las pruebas en relación con el número de vasos principales coloreados por fluoroscopia
ca_result_distribution = df.groupBy('ca', 'Result').count()
print("Distribución de resultados en relación con ca:")
ca_result_distribution.show()

# Detener la sesión de Spark
spark.stop()