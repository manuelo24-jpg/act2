from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, min, when, avg, count, datediff, unix_timestamp,
    round, concat, lit, floor
)

spark = SparkSession.builder.appName("gold").getOrCreate()

pedidos   = spark.read.parquet("silver/pedidos")
tracking  = spark.read.parquet("silver/tracking")

# Fecha de entrega real
entregas = (
    tracking.filter(col("evento") == "delivered")
            .groupBy("id_pedido")
            .agg(min("ts").alias("fecha_entrega"))
)

# Enriquecer pedidos
pedidos_gold = pedidos.join(entregas, "id_pedido", "left")

# Calcular minutos/horas reales
pedidos_gold = (
    pedidos_gold
        .withColumn("dias_retraso", datediff("fecha_entrega", "fecha_prometida"))
        .withColumn("on_time", when(col("dias_retraso") <= 0, 1).otherwise(0))
        .withColumn("puerta_a_puerta", datediff("fecha_entrega", "fecha_pedido"))
        # Diferencia real entre fechas (en horas)
        .withColumn("retraso_horas_real",
            (unix_timestamp("fecha_entrega") - unix_timestamp("fecha_prometida")) / 3600
        )
        .withColumn("puerta_a_puerta_horas_real",
            (unix_timestamp("fecha_entrega") - unix_timestamp("fecha_pedido")) / 3600
        )
)

# --------------------------
# MÉTRICAS DESCRIPTIVAS
# --------------------------

# ---- On-time rate (%) ----
on_time_rate = (
    pedidos_gold
        .agg(round(avg("on_time") * 100, 2).alias("on_time_rate_percent"))
        .withColumn("descripcion",
            concat(col("on_time_rate_percent"), lit("% de entregas a tiempo"))
        )
)

# ---- Retraso medio en días + horas + minutos ----
retraso_medio_horas = pedidos_gold.filter(col("retraso_horas_real") > 0) \
    .agg(avg("retraso_horas_real").alias("horas"))

retraso_medio = (
    retraso_medio_horas
        .withColumn("dias", floor(col("horas") / 24))
        .withColumn("horas_restantes", floor(col("horas") % 24))
        .withColumn("minutos", floor((col("horas") * 60) % 60))
        .withColumn(
            "descripcion",
            concat(
                lit("Retraso medio: "),
                col("dias"), lit(" días "),
                col("horas_restantes"), lit(" horas "),
                col("minutos"), lit(" minutos")
            )
        )
)

# ---- Puerta a puerta medio en días + horas + minutos ----
pta_puerta_horas = pedidos_gold \
    .agg(avg("puerta_a_puerta_horas_real").alias("horas"))

pta_puerta = (
    pta_puerta_horas
        .withColumn("dias", floor(col("horas") / 24))
        .withColumn("horas_restantes", floor(col("horas") % 24))
        .withColumn("minutos", floor((col("horas") * 60) % 60))
        .withColumn(
            "descripcion",
            concat(
                lit("Tiempo medio puerta a puerta: "),
                col("dias"), lit(" días "),
                col("horas_restantes"), lit(" horas "),
                col("minutos"), lit(" minutos")
            )
        )
)

# ---- Comparativa ----
comparativa = (
    pedidos_gold
        .groupBy("transportista", "provincia")
        .agg(
            count("*").alias("total_pedidos"),
            round(avg("on_time")*100, 2).alias("on_time_rate_percent"),
            avg("retraso_horas_real").alias("retraso_horas_raw"),
            avg("puerta_a_puerta_horas_real").alias("pta_puerta_horas_raw")
        )
        # Formato ON TIME con %
        .withColumn("on_time_rate", concat(col("on_time_rate_percent"), lit("%")))
        # Retraso promedio formato
        .withColumn("ret_dias", floor(col("retraso_horas_raw")/24))
        .withColumn("ret_horas", floor(col("retraso_horas_raw")%24))
        .withColumn("ret_min", floor((col("retraso_horas_raw")*60)%60))
        .withColumn(
            "retraso_promedio",
            concat(
                col("ret_dias"), lit(" días "),
                col("ret_horas"), lit(" horas "),
                col("ret_min"), lit(" minutos")
            )
        )
        # Puerta a puerta formato
        .withColumn("pta_dias", floor(col("pta_puerta_horas_raw")/24))
        .withColumn("pta_horas", floor(col("pta_puerta_horas_raw")%24))
        .withColumn("pta_min", floor((col("pta_puerta_horas_raw")*60)%60))
        .withColumn(
            "puerta_a_puerta_promedio",
            concat(
                col("pta_dias"), lit(" días "),
                col("pta_horas"), lit(" horas "),
                col("pta_min"), lit(" minutos")
            )
        )
        .select(
            "transportista", "provincia", "total_pedidos",
            "on_time_rate",
            "retraso_promedio",
            "puerta_a_puerta_promedio"
        )
)

print("----- ON TIME RATE ------")
on_time_rate.show(truncate=False)

print("----- RETRASO MEDIO ------")
retraso_medio.show(truncate=False)

print("----- PUERTA A PUERTA ------")
pta_puerta.show(truncate=False)

print("----- COMPARATIVA ")
comparativa.show(20, truncate=False)

pedidos_gold.write.mode("overwrite").parquet("gold/pedidos_gold")
comparativa.write.mode("overwrite").parquet("gold/comparativa")

spark.stop()
