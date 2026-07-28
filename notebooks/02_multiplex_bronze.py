# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # 02 - Multiplex Bronze Ingestion
# MAGIC
# MAGIC Ingest multiple HydroFlow raw entities into a single normalized Bronze Delta table.
# MAGIC
# MAGIC Each source record is preserved as JSON inside `raw_payload`.

# COMMAND ----------
from pyspark.sql.functions import (
    col,
    current_date,
    current_timestamp,
    lit,
    struct,
    to_json,
)

# COMMAND ----------
# Configuration

catalog_name = "hydroflow"
bronze_schema_name = "bronze"

raw_base_path = "/Volumes/hydroflow/raw/raw_files"
checkpoint_base_path = "/Volumes/hydroflow/bronze/checkpoints/multiplex"
schema_base_path = "/Volumes/hydroflow/bronze/schemas/multiplex"

target_table = f"{catalog_name}.{bronze_schema_name}.bronze_raw_events"

# COMMAND ----------
# Create Bronze schema if needed

spark.sql(
    f"CREATE SCHEMA IF NOT EXISTS "
    f"{catalog_name}.{bronze_schema_name}"
)

# COMMAND ----------
# Source definitions

bronze_sources = [
    {
        "source_entity": "meter_readings",
        "source_system": "ami_meter_gateway",
        "source_path": f"{raw_base_path}/meter_readings",
        "file_format": "json",
        "partition_columns": "ingest_date",
    },
    {
        "source_entity": "outage_events",
        "source_system": "outage_management_system",
        "source_path": f"{raw_base_path}/outage_events",
        "file_format": "json",
        "partition_columns": "ingest_date",
    },
    {
        "source_entity": "customer_cdc",
        "source_system": "customer_information_system",
        "source_path": f"{raw_base_path}/customer_cdc",
        "file_format": "json",
        "partition_columns": "ingest_date",
    },
    {
        "source_entity": "billing",
        "source_system": "billing_platform",
        "source_path": f"{raw_base_path}/billing",
        "file_format": "json",
        "partition_columns": "billing_month",
    },
    {
        "source_entity": "gis_zones",
        "source_system": "gis_reference_system",
        "source_path": f"{raw_base_path}/gis_zones",
        "file_format": "csv",
        "partition_columns": None,
    },
]

# COMMAND ----------
# Multiplex Bronze ingestion function

def ingest_multiplex_source(source_config: dict) -> None:
    source_entity = source_config["source_entity"]
    file_format = source_config["file_format"]

    reader = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", file_format)
        .option(
            "cloudFiles.schemaLocation",
            f"{schema_base_path}/{source_entity}",
        )
        .option("cloudFiles.inferColumnTypes", "true")
    )

    if source_config["partition_columns"]:
        reader = reader.option(
            "cloudFiles.partitionColumns",
            source_config["partition_columns"],
        )

    if file_format == "csv":
        reader = reader.option("header", "true")

    raw_df = reader.load(source_config["source_path"])

    payload_columns = [
        col(column_name)
        for column_name in raw_df.columns
    ]

    multiplex_df = raw_df.select(
        lit(source_entity).alias("source_entity"),
        lit(source_config["source_system"]).alias(
            "source_system"
        ),
        lit("v1").alias("schema_version"),
        to_json(
            struct(*payload_columns)
        ).alias("raw_payload"),
        col("_metadata.file_path").alias("_source_file"),
        current_timestamp().alias(
            "_ingestion_timestamp"
        ),
        current_date().alias("_load_date"),
    )

    query = (
        multiplex_df.writeStream
        .option(
            "checkpointLocation",
            f"{checkpoint_base_path}/{source_entity}",
        )
        .trigger(availableNow=True)
        .toTable(target_table)
    )

    query.awaitTermination()

    print(f"Multiplexed source: {source_entity}")

# COMMAND ----------
# Run Multiplex Bronze ingestion

for source in bronze_sources:
    ingest_multiplex_source(source)

# COMMAND ----------
# Validate Multiplex Bronze table

multiplex_table = spark.table(target_table)

print(f"{target_table}: {multiplex_table.count()} rows")

# COMMAND ----------
display(
    multiplex_table
    .groupBy("source_entity")
    .count()
    .orderBy("source_entity")
)

# COMMAND ----------
display(
    multiplex_table.select(
        "source_entity",
        "source_system",
        "schema_version",
        "raw_payload",
        "_source_file",
        "_ingestion_timestamp",
    ).limit(10)
)