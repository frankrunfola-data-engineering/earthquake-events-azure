# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Silver — Flatten + Clean
# MAGIC
# MAGIC Read Bronze JSON, normalize the nested GeoJSON into a tidy table, validate columns, and write Silver outputs to ADLS.
# MAGIC
# MAGIC Designed to run as a Databricks Job task called **Silver**.
# COMMAND ----------
from pyspark.sql import Window
from pyspark.sql import functions as F

bronze_output = dbutils.jobs.taskValues.get(taskKey="Bronze", key="bronze_output")
run_date = bronze_output.get("run_date")
bronze_features_path = bronze_output.get("bronze_features_path") or bronze_output.get(
    "legacy_features_path"
)
silver_adls = bronze_output.get("silver_adls")

assert run_date, "Missing run_date from bronze_output"
assert bronze_features_path, "Missing bronze_features_path from bronze_output"
assert silver_adls, "Missing silver_adls from bronze_output"

print(f"Reading Bronze features from: {bronze_features_path}")
df_raw = spark.read.json(bronze_features_path)
print("Raw schema:")
df_raw.printSchema()
# COMMAND ----------
# Flatten GeoJSON feature into tabular rows
df = df_raw.select(
    F.col("id").alias("event_id"),
    F.col("geometry.coordinates").getItem(0).cast("double").alias("longitude"),
    F.col("geometry.coordinates").getItem(1).cast("double").alias("latitude"),
    F.col("geometry.coordinates").getItem(2).cast("double").alias("elevation_km"),
    F.col("properties.title").alias("title"),
    F.col("properties.place").alias("place_description"),
    F.col("properties.sig").cast("int").alias("sig"),
    F.col("properties.mag").cast("double").alias("mag"),
    F.col("properties.magType").alias("mag_type"),
    F.col("properties.time").cast("long").alias("time_ms"),
    F.col("properties.updated").cast("long").alias("updated_ms"),
)

# Convert epoch-millis to timestamps
df = (
    df.withColumn("event_ts", F.to_timestamp(F.from_unixtime(F.col("time_ms") / 1000)))
    .withColumn("updated_ts", F.to_timestamp(F.from_unixtime(F.col("updated_ms") / 1000)))
    .withColumn("event_date", F.to_date("event_ts"))
    .withColumn("run_date", F.lit(run_date))
    .drop("time_ms", "updated_ms")
)

# Basic validation + cleaning
df = df.filter(F.col("event_id").isNotNull())
df = df.filter((F.col("latitude").between(-90, 90)) & (F.col("longitude").between(-180, 180)))
df = df.filter(F.col("event_ts").isNotNull())

# Dedupe by event_id, keep the most recently updated
w = Window.partitionBy("event_id").orderBy(F.col("updated_ts").desc_nulls_last())
df = df.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")

print("Silver preview:")
display(df.limit(10))
# COMMAND ----------
# Write Silver output (idempotent by run_date)
silver_output_path = f"{silver_adls}earthquake_events_silver/run_date={run_date}/"

(df.repartition("event_date").write.mode("overwrite").parquet(silver_output_path))

row_count = df.count()
print(f"Wrote {row_count} rows -> {silver_output_path}")

dbutils.jobs.taskValues.set(
    key="silver_output",
    value={
        "run_date": run_date,
        "silver_output_path": silver_output_path,
        "silver_row_count": int(row_count),
    },
)
