from pyspark.sql import SparkSession

# Crear SparkSession
spark = SparkSession.builder \
    .appName("EjemploNombresEdades") \
    .getOrCreate()

# Datos de prueba
data = [
    ("Ana", 2225),
    ("Luis", 32222),
    ("María", 2822),
    ("Carlos", 4220),
    ("Elena", 2222)
]

# Crear DataFrame
df = spark.createDataFrame(data, ["nombre", "saldo"])

# Mostrar el DataFrame
df.show()

# Ejemplo de operación: filtrar mayores de 30
df_mayores_30 = df.filter(df.saldo > 3020)
print(df_mayores_30.show())