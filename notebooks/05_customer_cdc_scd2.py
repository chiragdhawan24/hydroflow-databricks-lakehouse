# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # 05 - Customer CDC and SCD Type 2
# MAGIC
# MAGIC Parse customer CDC events from Multiplex Bronze, validate them,
# MAGIC and build a historical SCD Type 2 customer dimension.

# COMMAND ----------
from pyspark.sql.functions import (
    col,
    concat_ws,
    from_json,
    lead,
    lit,
    try_to_timestamp,
    when,
)
from pyspark.sql.types import (
    LongType,
    StringType,
    StructField,
    StructType,
)
from pyspark.sql.window import Window

# COMMAND ----------
# Configuration

catalog_name = "hydroflow"
bronze_schema_name = "bronze"
silver_schema_name = "silver"

bronze_raw_table = (
    f"{catalog_name}.{bronze_schema_name}.bronze_raw_events"
)

clean_cdc_table = (
    f"{catalog_name}.{silver_schema_name}.silver_customer_cdc_clean"
)

quarantine_table = (
    f"{catalog_name}.{silver_schema_name}."
    f"quarantine_invalid_customer_cdc"
)

scd2_table = (
    f"{catalog_name}.{silver_schema_name}.dim_customer_scd2"
)

valid_checkpoint = (
    "/Volumes/hydroflow/silver/checkpoints/customer_cdc_valid"
)

invalid_checkpoint = (
    "/Volumes/hydroflow/silver/checkpoints/customer_cdc_invalid"
)

# COMMAND ----------
# Customer CDC payload schema

customer_schema = StructType(
    [
        StructField("customer_id", StringType()),
        StructField("customer_name", StringType()),
        StructField("segment", StringType()),
        StructField("service_zone_id", StringType()),
        StructField("email", StringType()),
        StructField("phone", StringType()),
        StructField("address_line1", StringType()),
        StructField("city", StringType()),
        StructField("state", StringType()),
        StructField("postal_code", StringType()),
        StructField("account_status", StringType()),
        StructField("created_at", StringType()),
    ]
)

customer_cdc_schema = StructType(
    [
        StructField("cdc_event_id", StringType()),
        StructField("source_system", StringType()),
        StructField("source_entity", StringType()),
        StructField("operation", StringType()),
        StructField("sequence_num", LongType()),
        StructField("event_timestamp", StringType()),
        StructField("customer", customer_schema),
        StructField("ingest_date", StringType()),
    ]
)

# COMMAND ----------
# Read Customer CDC stream from Multiplex Bronze

customer_cdc_stream = (
    spark.readStream
    .table(bronze_raw_table)
    .filter(col("source_entity") == "customer_cdc")
)

# COMMAND ----------
# Parse CDC payload

parsed_cdc_stream = (
    customer_cdc_stream
    .withColumn(
        "cdc",
        from_json(
            col("raw_payload"),
            customer_cdc_schema,
        ),
    )
    .select(
        col("cdc.cdc_event_id").alias("cdc_event_id"),
        col("cdc.operation").alias("operation"),
        col("cdc.sequence_num").alias("sequence_num"),
        col("cdc.event_timestamp").alias(
            "event_timestamp_raw"
        ),
        try_to_timestamp(
            col("cdc.event_timestamp"),
            lit("yyyy-MM-dd'T'HH:mm:ssX"),
        ).alias("event_timestamp"),
        col("cdc.customer.customer_id").alias("customer_id"),
        col("cdc.customer.customer_name").alias("customer_name"),
        col("cdc.customer.segment").alias("segment"),
        col("cdc.customer.service_zone_id").alias(
            "service_zone_id"
        ),
        col("cdc.customer.email").alias("email"),
        col("cdc.customer.phone").alias("phone"),
        col("cdc.customer.address_line1").alias("address_line1"),
        col("cdc.customer.city").alias("city"),
        col("cdc.customer.state").alias("state"),
        col("cdc.customer.postal_code").alias("postal_code"),
        col("cdc.customer.account_status").alias(
            "account_status"
        ),
        col("cdc.ingest_date").alias("ingest_date"),
        col("_source_file"),
        col("_ingestion_timestamp"),
        col("_load_date"),
    )
)

# COMMAND ----------
# Validate CDC records

validated_cdc_stream = parsed_cdc_stream.withColumn(
    "quality_error",
    concat_ws(
        "; ",
        when(
            col("cdc_event_id").isNull(),
            lit("missing_cdc_event_id"),
        ),
        when(
            col("customer_id").isNull(),
            lit("missing_customer_id"),
        ),
        when(
            col("operation").isNull()
            | (~col("operation").isin("insert", "update", "delete")),
            lit("invalid_operation"),
        ),
        when(
            col("sequence_num").isNull(),
            lit("missing_sequence_num"),
        ),
        when(
            col("event_timestamp").isNull(),
            lit("invalid_event_timestamp"),
        ),
    ),
)

# COMMAND ----------
# Split valid and invalid CDC records

valid_cdc_stream = (
    validated_cdc_stream
    .filter(col("quality_error") == "")
    .drop("quality_error")
)

invalid_cdc_stream = (
    validated_cdc_stream
    .filter(col("quality_error") != "")
)

# COMMAND ----------
# Write clean CDC events

valid_query = (
    valid_cdc_stream.writeStream
    .option("checkpointLocation", valid_checkpoint)
    .trigger(availableNow=True)
    .toTable(clean_cdc_table)
)

invalid_query = (
    invalid_cdc_stream.writeStream
    .option("checkpointLocation", invalid_checkpoint)
    .trigger(availableNow=True)
    .toTable(quarantine_table)
)

valid_query.awaitTermination()
invalid_query.awaitTermination()

# COMMAND ----------
# MAGIC %md
# MAGIC ## Build SCD Type 2 History
# MAGIC
# MAGIC Each insert/update represents a customer version.
# MAGIC The next CDC event determines when that version expires.

# COMMAND ----------
history_window = (
    Window
    .partitionBy("customer_id")
    .orderBy(
        col("sequence_num"),
        col("event_timestamp"),
        col("cdc_event_id"),
    )
)

ordered_cdc = (
    spark.table(clean_cdc_table)
    .withColumn(
        "next_event_timestamp",
        lead("event_timestamp").over(history_window),
    )
    .withColumn(
        "next_operation",
        lead("operation").over(history_window),
    )
)

# COMMAND ----------
# Build dimension versions
#
# Delete events close the previous version but are not themselves
# stored as active customer dimension versions.

customer_scd2 = (
    ordered_cdc
    .filter(col("operation").isin("insert", "update"))
    .select(
        "customer_id",
        "customer_name",
        "segment",
        "service_zone_id",
        "email",
        "phone",
        "address_line1",
        "city",
        "state",
        "postal_code",
        "account_status",
        col("event_timestamp").alias(
            "effective_start_timestamp"
        ),
        col("next_event_timestamp").alias(
            "effective_end_timestamp"
        ),
        col("next_event_timestamp")
        .isNull()
        .alias("is_current"),
        (
            col("next_operation") == "delete"
        ).alias("closed_by_delete"),
        "sequence_num",
        "cdc_event_id",
    )
)

# COMMAND ----------
# Write SCD Type 2 dimension

(
    customer_scd2.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(scd2_table)
)

# COMMAND ----------
# Validate CDC operations

display(
    spark.table(clean_cdc_table)
    .groupBy("operation")
    .count()
    .orderBy("operation")
)

# COMMAND ----------
# Validate SCD Type 2 history

current_record_violations = (
    spark.table(scd2_table)
    .filter(col("is_current"))
    .groupBy("customer_id")
    .count()
    .filter(col("count") > 1)
    .count()
)

print(
    f"Clean CDC events: "
    f"{spark.table(clean_cdc_table).count()}"
)

print(
    f"SCD Type 2 versions: "
    f"{spark.table(scd2_table).count()}"
)

print(
    f"Customers with multiple current records: "
    f"{current_record_violations}"
)

assert current_record_violations == 0

# COMMAND ----------
# Inspect customer history

display(
    spark.table(scd2_table)
    .orderBy(
        "customer_id",
        "effective_start_timestamp",
    )
)