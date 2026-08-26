# Databricks notebook source
# MAGIC %md
# MAGIC # 06 - Delta Change Data Feed
# MAGIC
# MAGIC Enable Delta Change Data Feed on the customer SCD Type 2 dimension
# MAGIC and incrementally archive row-level changes for downstream processing.

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

# Configuration

catalog_name = "hydroflow"
silver_schema_name = "silver"

source_table = (
    f"{catalog_name}.{silver_schema_name}."
    f"dim_customer_scd2"
)

change_feed_table = (
    f"{catalog_name}.{silver_schema_name}."
    f"customer_change_feed"
)

checkpoint_path = (
    "/Volumes/hydroflow/silver/checkpoints/"
    "customer_scd2_cdf"
)

# COMMAND ----------

# Enable Change Data Feed on the source Delta table

spark.sql(
    f"""
    ALTER TABLE {source_table}
    SET TBLPROPERTIES (
        delta.enableChangeDataFeed = true
    )
    """
)

# COMMAND ----------

# Verify CDF is enabled

display(
    spark.sql(
        f"""
        SHOW TBLPROPERTIES {source_table}
        ('delta.enableChangeDataFeed')
        """
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read the Change Data Feed
# MAGIC
# MAGIC The first streaming run captures the current table snapshot as inserts.
# MAGIC Subsequent runs process only newly committed changes.

# COMMAND ----------

customer_cdf_stream = (
    spark.readStream
    .option("readChangeFeed", "true")
    .table(source_table)
)

# COMMAND ----------

# Persist the change feed for downstream use

query = (
    customer_cdf_stream.writeStream
    .option(
        "checkpointLocation",
        checkpoint_path,
    )
    .trigger(availableNow=True)
    .toTable(change_feed_table)
)

query.awaitTermination()

# COMMAND ----------

# Validate archived changes

change_feed_df = spark.table(change_feed_table)

print(
    f"Archived CDF records: "
    f"{change_feed_df.count()}"
)

# COMMAND ----------

# Inspect change types

display(
    change_feed_df
    .groupBy("_change_type")
    .count()
    .orderBy("_change_type")
)

# COMMAND ----------

# Inspect CDF metadata and customer changes

display(
    change_feed_df
    .select(
        "customer_id",
        "service_zone_id",
        "account_status",
        "is_current",
        "_change_type",
        "_commit_version",
        "_commit_timestamp",
    )
    .orderBy(
        col("_commit_version").desc(),
        col("customer_id"),
    )
)