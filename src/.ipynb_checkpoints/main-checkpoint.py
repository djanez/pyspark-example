from pyspark.sql import SparkSession


def main():
    # Crear la SparkSession
    spark = (
        SparkSession.builder
        .appName("pyspark-simple-example")
        .master("local[*]")
        .getOrCreate()
    )
    
    # Datos de ejemplo
    data = [
        ("Alice", 34),
        ("Bob", 45),
        ("Charlie", 29)
    ]
    
    # Crear DataFrame
    df = spark.createDataFrame(data, ["name", "age"])
    
    # Mostrar contenido
    df.show()
    
    # (Opcional) mostrar esquema
    df.printSchema()
    
    # Cerrar Spark
    spark.stop()

if __name__ == "__main__":
    main()
