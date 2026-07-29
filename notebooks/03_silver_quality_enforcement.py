# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # 03 - Silver Quality Enforcement
# MAGIC
# MAGIC Parse meter readings from the multiplex Bronze table, validate business rules,
# MAGIC route valid records to Silver, and quarantine invalid records.

# COMMAND ----------
from pyspark.sql.functions import (
    col,
    concat_ws,
    count,
    current_timestamp,
    from_json,
    lit,
    round,
    sum,
    # to_timestamp,
    try_to_timestamp,
    when,
)
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# COMMAND ----------
# Configuration

catalog_name = "hydroflow"
bronze_schema_name = "bronze"
silver_schema_name = "silver"

bronze_raw_table = (
    f"{catalog_name}.{bronze_schema_name}.bronze_raw_events"
)

silver_table = (
    f"{catalog_name}.{silver_schema_name}.silver_meter_readings"
)

quarantine_table = (
    f"{catalog_name}.{silver_schema_name}."
    f"quarantine_invalid_meter_readings"
)

metrics_table = (
    f"{catalog_name}.{silver_schema_name}.data_quality_metrics"
)

checkpoint_path = (
    "/Volumes/hydroflow/bronze/checkpoints/"
    "silver_quality_meter_readings"
)

# COMMAND ----------
# Create Silver schema

spark.sql(
    f"CREATE SCHEMA IF NOT EXISTS "
    f"{catalog_name}.{silver_schema_name}"
)

# COMMAND ----------
# Meter reading payload schema

meter_reading_schema = StructType(
    [
        StructField("event_id", StringType()),
        StructField("source_system", StringType()),
        StructField("source_entity", StringType()),
        StructField("event_timestamp", StringType()),
        StructField("meter_id", StringType()),
        StructField("customer_id", StringType()),
        StructField("service_zone_id", StringType()),
        StructField("meter_type", StringType()),
        StructField("usage_value", DoubleType()),
        StructField("usage_unit", StringType()),
        StructField("pressure_psi", DoubleType()),
        StructField("battery_pct", IntegerType()),
        StructField("firmware_version", StringType()),
        StructField("meter_status", StringType()),
        StructField("ingestion_hint", StringType()),
        StructField("is_intentionally_invalid", BooleanType()),
        StructField("duplicate_copy", BooleanType()),
        StructField("ingest_date", StringType()),
    ]
)

# COMMAND ----------
# Read meter events from Multiplex Bronze

bronze_meter_stream = (
    spark.readStream
    .table(bronze_raw_table)
    .filter(col("source_entity") == "meter_readings")
)

# COMMAND ----------
# Parse JSON payload into typed columns

parsed_stream = (
    bronze_meter_stream
    .withColumn(
        "meter",
        from_json(col("raw_payload"), meter_reading_schema),
    )
    .select(
        col("meter.event_id").alias("event_id"),
        col("meter.meter_id").alias("meter_id"),
        col("meter.customer_id").alias("customer_id"),
        col("meter.service_zone_id").alias("service_zone_id"),
        col("meter.meter_type").alias("meter_type"),
        col("meter.usage_value").alias("usage_value"),
        col("meter.usage_unit").alias("usage_unit"),
        col("meter.pressure_psi").alias("pressure_psi"),
        col("meter.battery_pct").alias("battery_pct"),
        col("meter.firmware_version").alias("firmware_version"),
        col("meter.meter_status").alias("meter_status"),
        col("meter.event_timestamp").alias(
            "event_timestamp_raw"
        ),
        try_to_timestamp(
            col("meter.event_timestamp"),
            lit("yyyy-MM-dd'T'HH:mm:ssX"),
        ).alias("event_timestamp"),
        col("meter.ingest_date").alias("ingest_date"),
        col("meter.duplicate_copy").alias("duplicate_copy"),
        col("meter.is_intentionally_invalid").alias(
            "is_intentionally_invalid"
        ),
        col("raw_payload"),
        col("source_system").alias("_source_system"),
        col("source_entity").alias("_source_entity"),
        col("_source_file"),
        col("_ingestion_timestamp"),
        col("_load_date"),
    )
)

# COMMAND ----------
# Apply quality rules
#
# The synthetic invalid flag is retained for testing but is not used
# to decide whether a record is valid.

validated_stream = parsed_stream.withColumn(
    "quality_error",
    concat_ws(
        "; ",
        when(
            col("event_id").isNull(),
            lit("missing_event_id"),
        ),
        when(
            col("meter_id").isNull(),
            lit("missing_meter_id"),
        ),
        when(
            col("customer_id").isNull(),
            lit("missing_customer_id"),
        ),
        when(
            col("service_zone_id").isNull(),
            lit("missing_service_zone_id"),
        ),
        when(
            col("event_timestamp").isNull(),
            lit("invalid_event_timestamp"),
        ),
        when(
            col("usage_value").isNull(),
            lit("missing_usage_value"),
        ),
        when(
            col("usage_value") < 0,
            lit("negative_usage_value"),
        ),
        when(
            col("meter_type").isNull()
            | (~col("meter_type").isin("water", "electric")),
            lit("invalid_meter_type"),
        ),
        when(
            col("meter_status").isNull()
            | (
                ~col("meter_status").isin(
                    "active",
                    "maintenance",
                    "offline",
                )
            ),
            lit("invalid_meter_status"),
        ),
        when(
            col("battery_pct").isNull()
            | (col("battery_pct") < 0)
            | (col("battery_pct") > 100),
            lit("invalid_battery_percentage"),
        ),
        when(
            (col("meter_type") == "water")
            & (
                col("pressure_psi").isNull()
                | (col("pressure_psi") <= 0)
            ),
            lit("invalid_water_pressure"),
        ),
    ),
)

# COMMAND ----------
# Write valid, invalid, and metric outputs per micro-batch

def write_quality_outputs(batch_df, batch_id: int) -> None:
    if batch_df.isEmpty():
        return

    valid_df = batch_df.filter(
        col("quality_error") == ""
    )

    invalid_df = batch_df.filter(
        col("quality_error") != ""
    )

    (
        valid_df
        .drop("quality_error", "raw_payload")
        .write
        .format("delta")
        .mode("append")
        .saveAsTable(silver_table)
    )

    (
        invalid_df
        .write
        .format("delta")
        .mode("append")
        .saveAsTable(quarantine_table)
    )

    metrics_df = (
        batch_df
        .agg(
            count(lit(1)).alias("total_records"),
            sum(
                when(
                    col("quality_error") == "",
                    1,
                ).otherwise(0)
            ).alias("valid_records"),
            sum(
                when(
                    col("quality_error") != "",
                    1,
                ).otherwise(0)
            ).alias("invalid_records"),
        )
        .withColumn("batch_id", lit(batch_id))
        .withColumn(
            "processed_at",
            current_timestamp(),
        )
        .withColumn(
            "invalid_rate_pct",
            round(
                col("invalid_records")
                / col("total_records")
                * 100,
                2,
            ),
        )
        .select(
            "batch_id",
            "processed_at",
            "total_records",
            "valid_records",
            "invalid_records",
            "invalid_rate_pct",
        )
    )

    (
        metrics_df
        .write
        .format("delta")
        .mode("append")
        .saveAsTable(metrics_table)
    )

# COMMAND ----------
# Execute streaming quality pipeline

query = (
    validated_stream.writeStream
    .foreachBatch(write_quality_outputs)
    .option("checkpointLocation", checkpoint_path)
    .trigger(availableNow=True)
    .start()
)

query.awaitTermination()

# COMMAND ----------
# Validate record reconciliation

bronze_count = (
    spark.table(bronze_raw_table)
    .filter(col("source_entity") == "meter_readings")
    .count()
)

silver_count = spark.table(silver_table).count()
quarantine_count = spark.table(quarantine_table).count()

print(f"Bronze meter readings: {bronze_count}")
print(f"Valid Silver records: {silver_count}")
print(f"Quarantined records: {quarantine_count}")

assert bronze_count == silver_count + quarantine_count

# COMMAND ----------
# Inspect quarantined records

display(
    spark.table(quarantine_table).select(
        "event_id",
        "meter_id",
        "event_timestamp_raw",
        "usage_value",
        "meter_status",
        "quality_error",
        "_source_file",
    )
)

# COMMAND ----------
# Inspect quality metrics

display(
    spark.table(metrics_table)
    .orderBy(col("processed_at").desc())
)