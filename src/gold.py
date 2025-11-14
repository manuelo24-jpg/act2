from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, col

spark = SparkSession.builder.appName("gold").getOrCreate()
ventas = spark.read.parquet("silver/ventas")
fact    = spark.read.parquet("silver/facturas_meta")

ventas_por_dia = ventas.groupBy("fecha").agg(
    sum("importe").alias("ventas_totales"),
    sum("unidades").alias("unidades_totales")
).orderBy("fecha")
ventas_por_dia.write.mode("overwrite").parquet("gold/ventas_por_dia")

top5 = ventas.groupBy("id_producto").agg(
    sum("unidades").alias("unidades_totales"),
    sum("importe").alias("importe_total")
).orderBy(col("unidades_totales").desc()).limit(5)
top5.write.mode("overwrite").parquet("gold/top5_productos")


ventas_agr = (ventas
    .groupBy("id_cliente", "fecha")
    .agg(
        sum("importe").alias("ventas_importe"),
        sum("unidades").alias("ventas_unidades")
    )
)

comparativa = (ventas_agr
    .join(fact, ["id_cliente", "fecha"], "left")
    .withColumn("diferencia", col("ventas_importe") - col("importe_total"))
)


comparativa.write.mode("overwrite").parquet("gold/comparativa_ventas_facturas")

df = spark.read.parquet("gold/comparativa_ventas_facturas") 
df.show(10, truncate=False)

print("Gold listo.")
spark.stop()

