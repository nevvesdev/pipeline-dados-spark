# Databricks notebook source
from functools import reduce
from pyspark.sql import functions as F

CATALOG = spark.sql("SELECT current_catalog()").first()[0]
BRONZE_TABLE = f"{CATALOG}.nyctaxi_bronze.trips"
SILVER_TABLE = f"{CATALOG}.nyctaxi_silver.trips"
QUALITY_TABLE = f"{CATALOG}.nyctaxi_silver.quality_log"

bronze = spark.table(BRONZE_TABLE)
total_bronze = bronze.count()
print(total_bronze)

# COMMAND ----------

exploration = bronze.withColumn(
    "duracao_min",
    (F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime")) / 60,
)

display(exploration.select("duracao_min", "trip_distance", "fare_amount").summary("min", "1%", "50%", "99%", "max"))

# COMMAND ----------

RULES = {
    "campos_obrigatorios_nulos": (
        F.col("fare_amount").isNull()
        | F.col("trip_distance").isNull()
        | F.col("tpep_pickup_datetime").isNull()
        | F.col("tpep_dropoff_datetime").isNull()
    ),
    "fare_amount_nao_positivo": F.col("fare_amount") <= 0,
    "trip_distance_nao_positiva": F.col("trip_distance") <= 0,
    "desembarque_nao_posterior_ao_embarque": F.col("tpep_dropoff_datetime") <= F.col("tpep_pickup_datetime"),
    "pickup_zip_nulo": F.col("pickup_zip").isNull(),
}

flags = {name: F.coalesce(cond, F.lit(False)) for name, cond in RULES.items()}
invalid_any = reduce(lambda a, b: a | b, flags.values())

profile = (
    bronze.agg(*[F.sum(flag.cast("int")).alias(name) for name, flag in flags.items()])
    .first()
    .asDict()
)

profile_df = spark.createDataFrame(
    [(name, int(count or 0)) for name, count in profile.items()],
    ["regra", "linhas_afetadas"],
)
display(profile_df.orderBy(F.desc("linhas_afetadas")))

# COMMAND ----------

silver = (
    bronze.filter(~invalid_any)
    .withColumnRenamed("tpep_pickup_datetime", "pickup_datetime")
    .withColumnRenamed("tpep_dropoff_datetime", "dropoff_datetime")
    .withColumn("pickup_date", F.to_date("pickup_datetime"))
    .withColumn("pickup_hour", F.hour("pickup_datetime"))
    .withColumn("pickup_dayofweek", F.dayofweek("pickup_datetime"))
    .withColumn(
        "trip_duration_min",
        F.round((F.unix_timestamp("dropoff_datetime") - F.unix_timestamp("pickup_datetime")) / 60, 2),
    )
    .withColumn("_silver_processed_at", F.current_timestamp())
    .select(
        "pickup_datetime",
        "dropoff_datetime",
        "pickup_date",
        "pickup_hour",
        "pickup_dayofweek",
        "trip_distance",
        "trip_duration_min",
        "fare_amount",
        "pickup_zip",
        "dropoff_zip",
        "_ingested_at",
        "_silver_processed_at",
    )
)

(
    silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(SILVER_TABLE)
)

# COMMAND ----------

silver_count = spark.table(SILVER_TABLE).count()

records = [(name, int(count or 0)) for name, count in profile.items()]
records += [
    ("total_bronze", total_bronze),
    ("total_silver", silver_count),
    ("total_descartadas", total_bronze - silver_count),
]

(
    spark.createDataFrame(records, ["regra", "linhas_afetadas"])
    .withColumn("executado_em", F.current_timestamp())
    .write.format("delta")
    .mode("append")
    .saveAsTable(QUALITY_TABLE)
)

display(spark.table(QUALITY_TABLE).orderBy(F.desc("executado_em")))

# COMMAND ----------

silver_table = spark.table(SILVER_TABLE)
violations = silver_table.filter(
    (F.col("fare_amount") <= 0)
    | (F.col("trip_distance") <= 0)
    | (F.col("trip_duration_min") <= 0)
).count()

print(violations)
silver_table.printSchema()
display(silver_table.limit(10))