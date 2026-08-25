# Databricks notebook source
# MAGIC %md
# MAGIC # 11 - Pipeline Validation
# MAGIC
# MAGIC Final validation task for the HydroFlow Lakeflow Job.
# MAGIC
# MAGIC This notebook verifies that core Bronze/Silver outputs exist
# MAGIC and satisfy basic pipeline invariants before the workflow is
# MAGIC considered successful.

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

tables_to_validate = [
    "hydroflow.bronze.bronze_meter_readings",
    "hydroflow.bronze.bronze_raw_events",
    "hydroflow.silver.silver_meter_readings",
    "hydroflow.silver.silver_meter_readings_deduped",
    "hydroflow.silver.dim_customer_scd2",
    "hydroflow.silver.customer_change_feed",
    "hydroflow.silver.silver_meter_readings_zoned",
    "hydroflow.silver.silver_billing",
]

# COMMAND ----------

table_counts = {}

for table_name in tables_to_validate:
    count = spark.table(table_name).count()
    table_counts[table_name] = count

    print(f"{table_name}: {count:,} rows")

# COMMAND ----------

# Core pipeline outputs must not be empty.

required_nonempty_tables = [
    "hydroflow.bronze.bronze_meter_readings",
    "hydroflow.bronze.bronze_raw_events",
    "hydroflow.silver.silver_meter_readings",
    "hydroflow.silver.silver_meter_readings_deduped",
    "hydroflow.silver.dim_customer_scd2",
    "hydroflow.silver.silver_meter_readings_zoned",
    "hydroflow.silver.silver_billing",
]

for table_name in required_nonempty_tables:
    assert table_counts[table_name] > 0, (
        f"Validation failed: {table_name} is empty."
    )

# COMMAND ----------

# Deduplication should never increase the meter-reading row count.

silver_count = table_counts[
    "hydroflow.silver.silver_meter_readings"
]

dedup_count = table_counts[
    "hydroflow.silver.silver_meter_readings_deduped"
]

assert dedup_count <= silver_count, (
    "Validation failed: deduplicated meter-reading count "
    "exceeds the pre-dedup Silver count."
)

# COMMAND ----------

# There must never be more than one current SCD Type 2 row
# for the same customer.

duplicate_current_customers = (
    spark.table("hydroflow.silver.dim_customer_scd2")
    .filter(col("is_current") == True)
    .groupBy("customer_id")
    .count()
    .filter(col("count") > 1)
    .count()
)

assert duplicate_current_customers == 0, (
    "Validation failed: at least one customer has "
    "multiple current SCD Type 2 records."
)

# COMMAND ----------

print("HydroFlow pipeline validation PASSED.")