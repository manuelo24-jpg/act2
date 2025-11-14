from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("bronze").getOrCreate()

pedidos = spark.read.option("header", True).option("delimiter", ";").csv("data/pedidos.csv")
tracking   = spark.read.option("multiLine", "true").json("data/tracking.json")

pedidos.show(5)

tracking.write.mode("overwrite").parquet("bronze/tracking")
pedidos.write.mode("overwrite").parquet("bronze/pedidos")


print("Bronze listo.")
spark.stop()

