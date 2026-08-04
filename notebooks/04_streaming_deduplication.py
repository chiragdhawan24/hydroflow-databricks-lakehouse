# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # 04 - Streaming Deduplication
# MAGIC
# MAGIC Remove duplicate meter-reading events from the validated Silver stream
# MAGIC using event-time watermarking and `event_id`.

# COMMAND ----------
from pyspark.sql.functions import (
    col,
    current_timestamp,
    lit,
)

# COMMAND ----------
# Configuration

catalog_name = "hydroflow"
silver_schema_name = "silver"

source_table = (
    f"{catalog_name}.{silver_schema_name}."
    f"silver_meter_readings"
)

target_table = (
    f"{catalog_name}.{silver_schema_name}."
    f"silver_meter_readings_deduped"
)

metrics_table = (
    f"{catalog_name}.{silver_schema_name}."
    f"deduplication_metrics"
)

checkpoint_path = (
    "/Volumes/hydroflow/silver/checkpoints/"
    "meter_readings_deduplication"
)

watermark_delay = "2 days"

# COMMAND ----------
# Create Silver schema and checkpoint volume

spark.sql(
    f"CREATE SCHEMA IF NOT EXISTS "
    f"{catalog_name}.{silver_schema_name}"
)

spark.sql(
    f"CREATE VOLUME IF NOT EXISTS "
    f"{catalog_name}.{silver_schema_name}.checkpoints"
)

# COMMAND ----------
# Read validated Silver meter readings

silver_meter_stream = (
    spark.readStream
    .table(source_table)
)

# COMMAND ----------
# Apply event-time watermark and deduplication

deduplicated_stream = (
    silver_meter_stream
    .withWatermark(
        "event_timestamp",
        watermark_delay,
    )
    .dropDuplicatesWithinWatermark(
        ["event_id"]
    )
)

# COMMAND ----------
# Write deduplicated records

query = (
    deduplicated_stream.writeStream
    .option(
        "checkpointLocation",
        checkpoint_path,
    )
    .trigger(availableNow=True)
    .toTable(target_table)
)

query.awaitTermination()

# COMMAND ----------
# Calculate deduplication metrics

source_count = spark.table(source_table).count()
deduplicated_count = spark.table(target_table).count()

duplicates_removed = (
    source_count - deduplicated_count
)

duplicate_rate_pct = (
    round(
        duplicates_removed
        / source_count
        * 100,
        2,
    )
    if source_count > 0
    else 0.0
)

metrics_df = (
    spark.range(1)
    .select(
        current_timestamp().alias("measured_at"),
        lit(source_count).alias("source_records"),
        lit(deduplicated_count).alias(
            "deduplicated_records"
        ),
        lit(duplicates_removed).alias(
            "duplicates_removed"
        ),
        lit(duplicate_rate_pct).alias(
            "duplicate_rate_pct"
        ),
        lit(watermark_delay).alias(
            "watermark_delay"
        ),
    )
)

(
    metrics_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(metrics_table)
)

# COMMAND ----------
# Validate results

remaining_duplicates = (
    spark.table(target_table)
    .groupBy("event_id")
    .count()
    .filter(col("count") > 1)
    .count()
)

print(f"Validated Silver records: {source_count}")
print(f"Deduplicated Silver records: {deduplicated_count}")
print(f"Duplicates removed: {duplicates_removed}")
print(f"Duplicate rate: {duplicate_rate_pct}%")
print(f"Remaining duplicate event IDs: {remaining_duplicates}")

assert deduplicated_count <= source_count
assert remaining_duplicates == 0

# COMMAND ----------
# Inspect deduplicated records

display(
    spark.table(target_table)
    .select(
        "event_id",
        "meter_id",
        "event_timestamp",
        "usage_value",
        "duplicate_copy",
        "_source_file",
    )
    .orderBy("event_timestamp")
)

# COMMAND ----------
# Inspect deduplication metrics

display(
    spark.table(metrics_table)
)