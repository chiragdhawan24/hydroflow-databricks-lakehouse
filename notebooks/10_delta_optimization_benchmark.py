# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # 10 - Delta Optimization Benchmark
# MAGIC
# MAGIC Build a controlled 1M-row benchmark dataset and compare:
# MAGIC
# MAGIC 1. An intentionally unclustered Delta baseline
# MAGIC 2. The same data reorganized using Liquid Clustering
# MAGIC
# MAGIC This benchmark is isolated from the real HydroFlow Silver/Gold tables.

# COMMAND ----------
from math import ceil

from pyspark.sql.functions import (
    col,
    concat,
    concat_ws,
    expr,
    lit,
    pmod,
    sha2,
    xxhash64,
)

# COMMAND ----------
# Configuration

catalog_name = "hydroflow"
benchmark_schema_name = "benchmark"

source_table = (
    f"{catalog_name}.silver."
    f"silver_meter_readings_zoned"
)

baseline_table = (
    f"{catalog_name}.{benchmark_schema_name}."
    f"meter_readings_baseline"
)

optimized_table = (
    f"{catalog_name}.{benchmark_schema_name}."
    f"meter_readings_optimized"
)

target_rows = 1_000_000

# Number of Spark partitions used to intentionally scatter the
# initial physical layout.
initial_write_partitions = 32

# Small target files are intentional for this controlled benchmark.
target_file_size = "32mb"

# COMMAND ----------
# Create isolated benchmark schema

spark.sql(
    f"""
    CREATE SCHEMA IF NOT EXISTS
    {catalog_name}.{benchmark_schema_name}
    """
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Remove previous benchmark attempt
# MAGIC
# MAGIC These are disposable benchmark tables only.
# MAGIC Real HydroFlow Silver/Gold tables are not touched.

# COMMAND ----------
spark.sql(
    f"DROP TABLE IF EXISTS {baseline_table}"
)

spark.sql(
    f"DROP TABLE IF EXISTS {optimized_table}"
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Generate 1M benchmark events
# MAGIC
# MAGIC Start from real HydroFlow Silver meter events.
# MAGIC Replicate them and shift timestamps over approximately one year.

# COMMAND ----------
source_df = spark.table(source_table)

source_count = source_df.count()

replication_factor = ceil(
    target_rows / source_count
)

print(f"Source rows: {source_count}")
print(f"Target rows: {target_rows}")
print(
    f"Replication factor: "
    f"{replication_factor}"
)

# COMMAND ----------
replicas_df = (
    spark.range(replication_factor)
    .withColumnRenamed(
        "id",
        "replica_id",
    )
)

# COMMAND ----------
expanded_df = (
    source_df
    .crossJoin(replicas_df)
    .limit(target_rows)
)

# COMMAND ----------
# Make the replicated event IDs unique and spread timestamps
# across roughly one year.

expanded_df = (
    expanded_df
    .withColumn(
        "event_id",
        concat(
            col("event_id"),
            lit("_"),
            col("replica_id").cast("string"),
        ),
    )
    .withColumn(
        "event_timestamp",
        expr(
            """
            timestampadd(
                DAY,
                CAST(replica_id % 365 AS INT),
                event_timestamp
            )
            """
        ),
    )
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Add benchmark-only payload
# MAGIC
# MAGIC The previous 1M-row dataset compressed to only ~4.7 MB because most
# MAGIC values were repeated.
# MAGIC
# MAGIC These deterministic high-entropy strings exist ONLY to make the
# MAGIC physical dataset large enough for a meaningful file-layout benchmark.
# MAGIC They are not part of HydroFlow's business data model.

# COMMAND ----------
def salted_hash(salt):
    return sha2(
        concat_ws(
            ":",
            col("event_id"),
            col("replica_id").cast("string"),
            lit(salt),
        ),
        256,
    )

# COMMAND ----------
benchmark_df = (
    expanded_df

    .withColumn(
        "benchmark_payload_a",
        concat_ws(
            "|",
            *[
                salted_hash(f"A{i}")
                for i in range(8)
            ],
        ),
    )

    .withColumn(
        "benchmark_payload_b",
        concat_ws(
            "|",
            *[
                salted_hash(f"B{i}")
                for i in range(8)
            ],
        ),
    )

    .drop("replica_id")
)

# COMMAND ----------
benchmark_count = benchmark_df.count()

print(
    f"Generated benchmark rows: "
    f"{benchmark_count}"
)

assert benchmark_count == target_rows

# COMMAND ----------
# MAGIC %md
# MAGIC ## Intentionally scatter the initial layout
# MAGIC
# MAGIC Both tables start from the SAME mixed physical ordering.
# MAGIC
# MAGIC The hash bucket deliberately distributes zones and timestamps across
# MAGIC different Spark partitions instead of naturally grouping them.

# COMMAND ----------
scattered_df = (
    benchmark_df

    .withColumn(
        "_benchmark_scatter_bucket",
        pmod(
            xxhash64(col("event_id")),
            lit(initial_write_partitions),
        ),
    )

    .repartition(
        initial_write_partitions,
        col("_benchmark_scatter_bucket"),
    )

    .drop("_benchmark_scatter_bucket")
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Create empty benchmark tables
# MAGIC
# MAGIC We explicitly:
# MAGIC
# MAGIC - target ~32 MB files
# MAGIC - collect statistics on the future clustering/filter columns
# MAGIC - disable predictive optimization so our controlled experiment
# MAGIC   is not automatically changed behind the scenes.

# COMMAND ----------
benchmark_df.createOrReplaceTempView(
    "hydroflow_benchmark_source"
)

# COMMAND ----------
for table_name in [
    baseline_table,
    optimized_table,
]:
    spark.sql(
        f"""
        CREATE TABLE {table_name}
        USING DELTA

        TBLPROPERTIES (
            'delta.targetFileSize' = '{target_file_size}',
            'delta.dataSkippingStatsColumns'
                = 'service_zone_id,event_timestamp'
        )

        AS
        SELECT *
        FROM hydroflow_benchmark_source
        WHERE 1 = 0
        """
    )

    spark.sql(
        f"""
        ALTER TABLE {table_name}
        DISABLE PREDICTIVE OPTIMIZATION
        """
    )

# COMMAND ----------
# MAGIC %md
# MAGIC ## Write identical unclustered data to both tables

# COMMAND ----------
(
    scattered_df.write
    .format("delta")
    .mode("append")
    .saveAsTable(baseline_table)
)

# COMMAND ----------
(
    scattered_df.write
    .format("delta")
    .mode("append")
    .saveAsTable(optimized_table)
)

# COMMAND ----------
# Validate identical data volume

baseline_count = (
    spark.table(baseline_table)
    .count()
)

optimized_count = (
    spark.table(optimized_table)
    .count()
)

print(
    f"Baseline rows:  "
    f"{baseline_count}"
)

print(
    f"Optimized rows: "
    f"{optimized_count}"
)

assert baseline_count == target_rows
assert optimized_count == target_rows

# COMMAND ----------
# MAGIC %md
# MAGIC ## Collect Delta data-skipping statistics
# MAGIC
# MAGIC Both tables receive equivalent statistics before optimization.

# COMMAND ----------
spark.sql(
    f"""
    ANALYZE TABLE {baseline_table}
    COMPUTE DELTA STATISTICS
    """
)

spark.sql(
    f"""
    ANALYZE TABLE {optimized_table}
    COMPUTE DELTA STATISTICS
    """
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Inspect PRE-optimization physical layout

# COMMAND ----------
baseline_before = spark.sql(
    f"""
    DESCRIBE DETAIL {baseline_table}
    """
)

optimized_before = spark.sql(
    f"""
    DESCRIBE DETAIL {optimized_table}
    """
)

print("BASELINE BEFORE OPTIMIZATION")
display(baseline_before)

print("OPTIMIZED TABLE BEFORE CLUSTERING")
display(optimized_before)

# COMMAND ----------
# Programmatic physical-layout check

baseline_detail = (
    spark.sql(
        f"DESCRIBE DETAIL {baseline_table}"
    )
    .first()
)

print(
    f"Baseline physical size: "
    f"{baseline_detail['sizeInBytes']:,} bytes"
)

print(
    f"Baseline Delta files: "
    f"{baseline_detail['numFiles']}"
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Enable Liquid Clustering
# MAGIC
# MAGIC HydroFlow analytical queries commonly filter by:
# MAGIC
# MAGIC - service_zone_id
# MAGIC - event_timestamp

# COMMAND ----------
spark.sql(
    f"""
    ALTER TABLE {optimized_table}

    CLUSTER BY (
        service_zone_id,
        event_timestamp
    )
    """
)

# COMMAND ----------
# Confirm that clustering metadata is registered.

display(
    spark.sql(
        f"""
        DESCRIBE DETAIL {optimized_table}
        """
    )
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Force the initial physical reclustering
# MAGIC
# MAGIC FULL is intentional here because clustering has just been enabled
# MAGIC on data that was previously written without clustering.

# COMMAND ----------
optimization_result = spark.sql(
    f"""
    OPTIMIZE {optimized_table} FULL
    """
)

display(optimization_result)

# COMMAND ----------
# Recompute statistics after the physical rewrite.

spark.sql(
    f"""
    ANALYZE TABLE {optimized_table}
    COMPUTE DELTA STATISTICS
    """
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Inspect POST-optimization layout

# COMMAND ----------
print("BASELINE TABLE")

display(
    spark.sql(
        f"""
        DESCRIBE DETAIL {baseline_table}
        """
    )
)

# COMMAND ----------
print("LIQUID-CLUSTERED TABLE")

display(
    spark.sql(
        f"""
        DESCRIBE DETAIL {optimized_table}
        """
    )
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Inspect Delta transaction history

# COMMAND ----------
display(
    spark.sql(
        f"""
        DESCRIBE HISTORY {optimized_table}
        """
    )
)

# COMMAND ----------
# Final correctness validation

assert (
    spark.table(baseline_table).count()
    ==
    spark.table(optimized_table).count()
)

print(
    "Corrected Step 10 benchmark "
    "created successfully."
)