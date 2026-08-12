# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # 08 - Prepare Silver Billing
# MAGIC
# MAGIC Normalize and validate Bronze billing records before they are consumed
# MAGIC by Gold analytics.

# COMMAND ----------
from pyspark.sql.functions import (
    col,
    concat_ws,
    lit,
    to_date,
    try_to_timestamp,
    when,
)

# COMMAND ----------
catalog_name = "hydroflow"
bronze_schema_name = "bronze"
silver_schema_name = "silver"

bronze_billing_table = f"{catalog_name}.{bronze_schema_name}.bronze_billing"
silver_billing_table = f"{catalog_name}.{silver_schema_name}.silver_billing"
billing_quarantine_table = (
    f"{catalog_name}.{silver_schema_name}.quarantine_invalid_billing"
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Normalize Bronze billing data

# COMMAND ----------
billing_df = (
    spark.table(bronze_billing_table)
    .select(
        col("bill_id"),
        col("source_system"),
        col("source_entity"),
        col("customer_id"),
        col("service_zone_id"),
        to_date(col("billing_month")).alias("billing_month"),
        col("usage_gallons").cast("double").alias("usage_gallons"),
        col("bill_amount_usd").cast("double").alias("bill_amount_usd"),
        col("payment_status"),
        try_to_timestamp(
            col("generated_timestamp"),
            lit("yyyy-MM-dd'T'HH:mm:ssX"),
        ).alias("generated_timestamp"),
        col("_source_file"),
        col("_ingestion_timestamp"),
        col("_load_date"),
    )
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Apply billing quality rules

# COMMAND ----------
validated_billing = billing_df.withColumn(
    "quality_error",
    concat_ws(
        "; ",
        when(col("bill_id").isNull(), lit("missing_bill_id")),
        when(col("customer_id").isNull(), lit("missing_customer_id")),
        when(
            col("service_zone_id").isNull(),
            lit("missing_service_zone_id"),
        ),
        when(
            col("billing_month").isNull(),
            lit("invalid_billing_month"),
        ),
        when(
            col("usage_gallons").isNull() | (col("usage_gallons") < 0),
            lit("invalid_usage_gallons"),
        ),
        when(
            col("bill_amount_usd").isNull() | (col("bill_amount_usd") < 0),
            lit("invalid_bill_amount"),
        ),
        when(
            col("payment_status").isNull()
            | (~col("payment_status").isin("paid", "late", "unpaid")),
            lit("invalid_payment_status"),
        ),
        when(
            col("generated_timestamp").isNull(),
            lit("invalid_generated_timestamp"),
        ),
    ),
)

# COMMAND ----------
valid_billing = (
    validated_billing
    .filter(col("quality_error") == "")
    .drop("quality_error")
)

invalid_billing = validated_billing.filter(col("quality_error") != "")

# COMMAND ----------
(
    valid_billing.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(silver_billing_table)
)

# COMMAND ----------
(
    invalid_billing.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(billing_quarantine_table)
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Validation

# COMMAND ----------
bronze_count = spark.table(bronze_billing_table).count()
silver_count = spark.table(silver_billing_table).count()
quarantine_count = spark.table(billing_quarantine_table).count()

print(f"Bronze billing rows:       {bronze_count}")
print(f"Valid Silver rows:         {silver_count}")
print(f"Quarantined billing rows:  {quarantine_count}")

assert bronze_count == silver_count + quarantine_count

# COMMAND ----------
display(
    spark.table(silver_billing_table)
    .orderBy("customer_id")
    .limit(20)
)

# COMMAND ----------
display(
    spark.table(billing_quarantine_table)
    .select(
        "bill_id",
        "customer_id",
        "service_zone_id",
        "quality_error",
    )
)
