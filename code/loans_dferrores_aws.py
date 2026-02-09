from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, array, concat_ws, abs

# Inicializar Spark
spark = SparkSession.builder.appName("ErroresLoans").getOrCreate()

# rutas:
input_path = "s3://webinar-djb/data/bronze/loans/loans.csv"
output_path = "s3://webinar-djb/data/bronze/loans/errors/loans_errors.csv"

# Cargar datos
df = spark.read.option("header", True).option("inferSchema", True).csv(input_path)

# Definir columnas de error
df = df.withColumn(
    "error_num_payed_gt_num_fees",
    when(col("num_payed_fees") > col("num_fees"), "num_payed_fees > num_fees")
)

df = df.withColumn(
    "error_active_true_with_end_date",
    when((col("active") == True) & (col("end_date").isNotNull()), "active TRUE con end_date")
)


df = df.withColumn(
    "error_active_false_no_end_date",
    when((col("active") == False) & (col("end_date").isNull()), "active FALSE sin end_date")
)

df = df.withColumn(
    "expected_total_returned",
    (col("num_payed_fees") / col("num_fees")) * col("total_amount")
)

df = df.withColumn(
    "error_total_returned_incorrect",
    when(abs((col("total_returned") - col("expected_total_returned")).cast("double")) > 0.01, "total_returned incorrecto")
)

df = df.withColumn(
    "error_total_amount_incorrect",
    when(col("total_amount") != col("capital"), "total_amount incorrecto")
)

df = df.withColumn(
    "error_defaulter_paid",
    when((col("status") == "defaulter") & (col("num_payed_fees") == col("num_fees")), "defaulter con todo pagado")
)

error_cols = [
    "error_num_payed_gt_num_fees",
    "error_active_true_with_end_date",
    "error_active_false_no_end_date",
    "error_total_returned_incorrect",
    "error_total_amount_incorrect",
    "error_defaulter_paid"
]

df_errores = df.withColumn(
    "errores",
    concat_ws(", ", *[col(c) for c in error_cols])
).filter(col("errores") != "")

df_errores = df_errores.select("customer_id", "num_fees", "num_payed_fees", "total_amount", "total_returned", "status", "errores")

df_errores.write.mode("overwrite").option("header", True).csv(output_path)

spark.stop()
