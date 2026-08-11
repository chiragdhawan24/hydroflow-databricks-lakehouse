# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # 07 - Streaming Joins
# MAGIC
# MAGIC Enrich deduplicated meter readings with static GIS/service-zone data,
# MAGIC then correlate meter readings with streaming outage events.

# COMMAND ----------
from pyspark.sql.functions import (
    col,
    expr,
    from_json,
    lit,
    try_to_timestamp,
)
from pyspark.sql.types import (
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

meter_source_table = (
    f"{catalog_name}.{silver_schema_name}."
    f"silver_meter_readings_deduped"
)

gis_table = (
    f"{catalog_name}.{bronze_schema_name}."
    f"bronze_gis_zones"
)

bronze_raw_table = (
    f"{catalog_name}.{bronze_schema_name}."
    f"bronze_raw_events"
)

zoned_meter_table = (
    f"{catalog_name}.{silver_schema_name}."
    f"silver_meter_readings_zoned"
)

outage_match_table = (
    f"{catalog_name}.{silver_schema_name}."
    f"silver_meter_outage_matches"
)

zoned_checkpoint = (
    "/Volumes/hydroflow/silver/checkpoints/"
    "meter_readings_zoned"
)

outage_join_checkpoint = (
    "/Volumes/hydroflow/silver/checkpoints/"
    "meter_outage_join"
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Stream-Static Join
# MAGIC
# MAGIC Join streaming meter readings with the static GIS/service-zone reference table.

# COMMAND ----------
# Read meter readings as stream

meter_stream = (
    spark.readStream
    .table(meter_source_table)
)

# COMMAND ----------
# Read GIS zones as static DataFrame

gis_static = (
    spark.table(gis_table)
    .select(
        col("zone_id").alias("gis_zone_id"),
        "zone_name",
        "city",
        "state",
        "pressure_band",
        "service_priority",
        "centroid_latitude",
        "centroid_longitude",
    )
)

# COMMAND ----------
# Join streaming meter readings with static GIS data

zoned_meter_stream = (
    meter_stream.alias("meter")
    .join(
        gis_static.alias("gis"),
        col("meter.service_zone_id")
        == col("gis.gis_zone_id"),
        "left",
    )
    .select(
        col("meter.*"),
        col("gis.zone_name"),
        col("gis.city").alias("zone_city"),
        col("gis.state").alias("zone_state"),
        col("gis.pressure_band"),
        col("gis.service_priority"),
        col("gis.centroid_latitude"),
        col("gis.centroid_longitude"),
    )
)

# COMMAND ----------
# Write stream-static join output

zoned_query = (
    zoned_meter_stream.writeStream
    .option(
        "checkpointLocation",
        zoned_checkpoint,
    )
    .trigger(availableNow=True)
    .toTable(zoned_meter_table)
)

zoned_query.awaitTermination()

# COMMAND ----------
# Validate stream-static join

print(
    f"Zoned meter readings: "
    f"{spark.table(zoned_meter_table).count()}"
)

display(
    spark.table(zoned_meter_table)
    .select(
        "event_id",
        "meter_id",
        "service_zone_id",
        "zone_name",
        "zone_city",
        "service_priority",
        "event_timestamp",
    )
    .limit(10)
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Stream-Stream Join
# MAGIC
# MAGIC Correlate streaming meter readings with outage events for the same meter
# MAGIC within a bounded event-time window.

# COMMAND ----------
# Outage payload schema

outage_schema = StructType(
    [
        StructField("event_id", StringType()),
        StructField("source_system", StringType()),
        StructField("source_entity", StringType()),
        StructField("event_timestamp", StringType()),
        StructField("outage_id", StringType()),
        StructField("meter_id", StringType()),
        StructField("customer_id", StringType()),
        StructField("service_zone_id", StringType()),
        StructField("event_type", StringType()),
        StructField("severity", StringType()),
        StructField(
            "estimated_duration_minutes",
            IntegerType(),
        ),
        StructField("resolved_timestamp", StringType()),
        StructField("ingest_date", StringType()),
    ]
)

# COMMAND ----------
# Read zoned meter readings as stream

zoned_stream = (
    spark.readStream
    .table(zoned_meter_table)
    .select(
        "*",
        col("event_timestamp").alias(
            "meter_event_timestamp"
        ),
    )
    .withWatermark(
        "meter_event_timestamp",
        "2 days",
    )
)

# COMMAND ----------
# Read and parse outage events as stream

outage_stream = (
    spark.readStream
    .table(bronze_raw_table)
    .filter(
        col("source_entity") == "outage_events"
    )
    .withColumn(
        "outage",
        from_json(
            col("raw_payload"),
            outage_schema,
        ),
    )
    .select(
        col("outage.outage_id").alias("outage_id"),
        col("outage.meter_id").alias(
            "outage_meter_id"
        ),
        col("outage.event_type").alias(
            "outage_event_type"
        ),
        col("outage.severity").alias(
            "outage_severity"
        ),
        col(
            "outage.estimated_duration_minutes"
        ).alias("estimated_duration_minutes"),
        try_to_timestamp(
            col("outage.event_timestamp"),
            lit("yyyy-MM-dd'T'HH:mm:ssX"),
        ).alias("outage_event_timestamp"),
        try_to_timestamp(
            col("outage.resolved_timestamp"),
            lit("yyyy-MM-dd'T'HH:mm:ssX"),
        ).alias("resolved_timestamp"),
    )
    .withWatermark(
        "outage_event_timestamp",
        "2 days",
    )
)

# COMMAND ----------
# Stream-stream join condition
#
# Match:
# 1. Same meter
# 2. Outage occurs within +/- 6 hours of a meter reading

join_condition = expr(
    """
    meter.meter_id = outage.outage_meter_id
    AND outage.outage_event_timestamp
        >= meter.meter_event_timestamp - INTERVAL 6 HOURS
    AND outage.outage_event_timestamp
        <= meter.meter_event_timestamp + INTERVAL 6 HOURS
    """
)

# COMMAND ----------
# Perform stream-stream join

meter_outage_stream = (
    zoned_stream.alias("meter")
    .join(
        outage_stream.alias("outage"),
        join_condition,
        "inner",
    )
    .select(
        col("meter.event_id").alias(
            "meter_event_id"
        ),
        col("meter.meter_id"),
        col("meter.customer_id"),
        col("meter.service_zone_id"),
        col("meter.zone_name"),
        col("meter.usage_value"),
        col("meter.pressure_psi"),
        col("meter.meter_event_timestamp"),
        col("outage.outage_id"),
        col("outage.outage_event_type"),
        col("outage.outage_severity"),
        col(
            "outage.estimated_duration_minutes"
        ),
        col("outage.outage_event_timestamp"),
        col("outage.resolved_timestamp"),
    )
)

# COMMAND ----------
# Write stream-stream join output

outage_join_query = (
    meter_outage_stream.writeStream
    .option(
        "checkpointLocation",
        outage_join_checkpoint,
    )
    .trigger(availableNow=True)
    .toTable(outage_match_table)
)

outage_join_query.awaitTermination()

# COMMAND ----------
# Validate stream-stream join

print(
    f"Meter/outage matches: "
    f"{spark.table(outage_match_table).count()}"
)

display(
    spark.table(outage_match_table)
    .orderBy("outage_event_timestamp")
)