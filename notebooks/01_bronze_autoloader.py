# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # 01 - Bronze Auto Loader Ingestion
# MAGIC
# MAGIC Ingest HydroFlow raw files from Unity Catalog Volumes into Bronze Delta tables using Databricks Auto Loader.

# COMMAND ----------
from pyspark.sql.functions import col, current_date, current_timestamp, lit

# COMMAND ----------
# Configuration

catalog_name = "hydroflow"
raw_schema_name = "raw"
bronze_schema_name = "bronze"

raw_base_path = "/Volumes/hydroflow/raw/raw_files"
checkpoint_base_path = "/Volumes/hydroflow/bronze/checkpoints"
schema_base_path = "/Volumes/hydroflow/bronze/schemas"

# COMMAND ----------
# Create schemas if needed

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{raw_schema_name}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{bronze_schema_name}")

# COMMAND ----------
# Source definitions

bronze_sources = [
    {
        "source_entity": "meter_readings",
        "source_system": "ami_meter_gateway",
        "source_path": f"{raw_base_path}/meter_readings",
        "file_format": "json",
        "partition_columns": "ingest_date",
        "target_table": f"{catalog_name}.{bronze_schema_name}.bronze_meter_readings",
    },
    {
        "source_entity": "outage_events",
        "source_system": "outage_management_system",
        "source_path": f"{raw_base_path}/outage_events",
        "file_format": "json",
        "partition_columns": "ingest_date",
        "target_table": f"{catalog_name}.{bronze_schema_name}.bronze_outage_events",
    },
    {
        "source_entity": "customer_cdc",
        "source_system": "customer_information_system",
        "source_path": f"{raw_base_path}/customer_cdc",
        "file_format": "json",
        "partition_columns": "ingest_date",
        "target_table": f"{catalog_name}.{bronze_schema_name}.bronze_customer_cdc",
    },
    {
        "source_entity": "billing",
        "source_system": "billing_platform",
        "source_path": f"{raw_base_path}/billing",
        "file_format": "json",
        "partition_columns": "billing_month",
        "target_table": f"{catalog_name}.{bronze_schema_name}.bronze_billing",
    },
    {
        "source_entity": "gis_zones",
        "source_system": "gis_reference_system",
        "source_path": f"{raw_base_path}/gis_zones",
        "file_format": "csv",
        "partition_columns": None,
        "target_table": f"{catalog_name}.{bronze_schema_name}.bronze_gis_zones",
    },
]

# COMMAND ----------
# Auto Loader ingestion function

def ingest_bronze_source(source_config: dict) -> None:
    source_entity = source_config["source_entity"]
    file_format = source_config["file_format"]

    reader = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", file_format)
        .option("cloudFiles.schemaLocation", f"{schema_base_path}/{source_entity}")
        .option("cloudFiles.inferColumnTypes", "true")
    )

    if source_config["partition_columns"]:
        reader = reader.option(
            "cloudFiles.partitionColumns",
            source_config["partition_columns"]
        )

    if file_format == "csv":
        reader = reader.option("header", "true")

    bronze_df = (
        reader.load(source_config["source_path"])
        .withColumn("_source_file", col("_metadata.file_path"))
        .withColumn("_source_system", lit(source_config["source_system"]))
        .withColumn("_source_entity", lit(source_entity))
        .withColumn("_ingestion_timestamp", current_timestamp())
        .withColumn("_load_date", current_date())
    )

    query = (
        bronze_df.writeStream
        .option("checkpointLocation", f"{checkpoint_base_path}/{source_entity}")
        .trigger(availableNow=True)
        .toTable(source_config["target_table"])
    )

    query.awaitTermination()

    print(f"Loaded {source_config['target_table']}")

# COMMAND ----------
# Run Bronze ingestion

for source in bronze_sources:
    ingest_bronze_source(source)

# COMMAND ----------
# Validate Bronze tables

for source in bronze_sources:
    table_name = source["target_table"]
    row_count = spark.table(table_name).count()
    print(f"{table_name}: {row_count} rows")

# COMMAND ----------
display(spark.sql(f"SHOW TABLES IN {catalog_name}.{bronze_schema_name}"))