from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, coalesce, to_timestamp
from pyspark.sql.types import IntegerType

spark = SparkSession.builder.appName("silver").getOrCreate()

pedidos   = spark.read.parquet("bronze/pedidos")
tracking   = spark.read.parquet("bronze/tracking")

pedidos = (pedidos
    .withColumn("id_pedido",   col("id_pedido").cast(IntegerType()))
    .withColumn("id_cliente", col("id_cliente").cast(IntegerType()))
    .withColumn("fecha_pedido",coalesce(
    to_date(col("fecha_pedido"), "yyyy-MM-dd"),
    to_date(col("fecha_pedido"), "yyyy/MM/dd"),
    to_date(col("fecha_pedido"), "dd/MM/yyyy"),
    to_date(col("fecha_pedido"), "dd-MM-yyyy")
))
    .withColumn("fecha_prometida",coalesce(
    to_date(col("fecha_prometida"), "yyyy-MM-dd"),
    to_date(col("fecha_prometida"), "dd/MM/yyyy")
))
    .dropDuplicates(["id_pedido"])
)

tracking = (
    tracking
    .withColumn(
        "ts",
        coalesce(
            to_timestamp(col("ts"), "yyyy-MM-dd'T'HH:mm:ssX")
        )
    )
    .withColumn("id_pedido", col("id_pedido").cast(IntegerType()))
)

tracking.write.mode("overwrite").parquet("silver/tracking")
pedidos.write.mode("overwrite").parquet("silver/pedidos")

print("Mostrando datos de tracking")
tracking.show(10)
print("Mostrando datos de pedidos")
pedidos.show(10)

print("Silver listo.")
spark.stop()


