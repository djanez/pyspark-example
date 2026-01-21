from pyspark.sql import SparkSession

# Crear SparkSession
spark = SparkSession.builder \
    .appName("EjemploNombresEdades") \
    .getOrCreate()

# Datos de prueba
data = [
    ("Ana", 25),
    ("Luis", 32),
    ("María", 28),
    ("Carlos", 40),
    ("Elena", 22)
]

# Crear DataFrame
df = spark.createDataFrame(data, ["nombre", "edad"])

# Mostrar el DataFrame
df.show()

# Ejemplo de operación: filtrar mayores de 30
df_mayores_30 = df.filter(df.edad > 30)
print(df_mayores_30.show())