# Databricks notebook source
from pyspark.sql import functions as F

CATALOG = spark.sql("SELECT current_catalog()").first()[0]
SOURCE_TABLE = "samples.nyctaxi.trips"
TARGET_TABLE = f"{CATALOG}.nyctaxi_bronze.trips"

# COMMAND ----------

raw = spark.table(SOURCE_TABLE)

bronze = (
    raw
    .withColumn("_source_table", F.lit(SOURCE_TABLE))
    .withColumn("_ingested_at", F.current_timestamp())
)

(
    bronze.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TARGET_TABLE)
)

# COMMAND ----------

bronze_table = spark.table(TARGET_TABLE)
print(bronze_table.count())
bronze_table.printSchema()
display(bronze_table.limit(10))

# COMMAND ----------

display(spark.sql(f"DESCRIBE HISTORY {TARGET_TABLE}"))