# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # 01 - Bronze Auto Loader Ingestion
# MAGIC
# MAGIC This notebook will be implemented in Step 2. It will ingest raw HydroFlow files with Databricks Auto Loader using the `cloudFiles` source.
# MAGIC
# MAGIC Planned outputs:
# MAGIC - bronze_meter_readings
# MAGIC - bronze_outage_events
# MAGIC - bronze_customer_cdc
# MAGIC - bronze_billing

# COMMAND ----------
# Parameters to configure in Databricks later.
raw_base_path = "dbfs:/FileStore/hydroflow/raw"
checkpoint_base_path = "dbfs:/FileStore/hydroflow/checkpoints"
schema_base_path = "dbfs:/FileStore/hydroflow/schemas"
catalog_name = "hydroflow"
schema_name = "bronze"

# COMMAND ----------
# Placeholder for Step 2 implementation.
# Example shape:
# (
#   spark.readStream
#        .format("cloudFiles")
#        .option("cloudFiles.format", "json")
#        .option("cloudFiles.schemaLocation", f"{schema_base_path}/meter_readings")
#        .load(f"{raw_base_path}/meter_readings")
#        .writeStream
#        .option("checkpointLocation", f"{checkpoint_base_path}/meter_readings")
#        .toTable(f"{catalog_name}.{schema_name}.bronze_meter_readings")
# )
print("Step 2 will implement Bronze Auto Loader ingestion here.")
