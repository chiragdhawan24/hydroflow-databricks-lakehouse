-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 10 - Query Profile Benchmarks
-- MAGIC
-- MAGIC Compare identical analytical queries against:
-- MAGIC
-- MAGIC 1. Unclustered baseline Delta table
-- MAGIC 2. Liquid-clustered optimized Delta table

-- COMMAND ----------
-- IMPORTANT:
-- Disable result caching during benchmark runs.
--
-- Otherwise Databricks might simply return a previously cached result
-- instead of executing the query again.

SET use_cached_result = false;

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## Benchmark Query 1A - Baseline
-- MAGIC
-- MAGIC Selective zone + event-time analytical query.

-- COMMAND ----------
SELECT
    service_zone_id,
    COUNT(*) AS reading_count,
    ROUND(
        SUM(usage_value),
        2
    ) AS total_usage,
    ROUND(
        AVG(usage_value),
        2
    ) AS average_usage,
    ROUND(
        AVG(pressure_psi),
        2
    ) AS average_pressure_psi

FROM hydroflow.benchmark.meter_readings_baseline

WHERE service_zone_id = 'zone_001'

  AND event_timestamp >=
      TIMESTAMP '2027-03-01 00:00:00'

  AND event_timestamp <
      TIMESTAMP '2027-04-01 00:00:00'

GROUP BY service_zone_id;

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## Benchmark Query 1B - Optimized
-- MAGIC
-- MAGIC EXACT same query against the optimized table.

-- COMMAND ----------
SELECT
    service_zone_id,
    COUNT(*) AS reading_count,
    ROUND(
        SUM(usage_value),
        2
    ) AS total_usage,
    ROUND(
        AVG(usage_value),
        2
    ) AS average_usage,
    ROUND(
        AVG(pressure_psi),
        2
    ) AS average_pressure_psi

FROM hydroflow.benchmark.meter_readings_optimized

WHERE service_zone_id = 'zone_001'

  AND event_timestamp >=
      TIMESTAMP '2027-03-01 00:00:00'

  AND event_timestamp <
      TIMESTAMP '2027-04-01 00:00:00'

GROUP BY service_zone_id;

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## Benchmark Query 2A - Dashboard-style Baseline

-- COMMAND ----------
SELECT
    TO_DATE(event_timestamp) AS usage_date,
    zone_name,
    meter_type,

    COUNT(*) AS reading_count,

    ROUND(
        SUM(usage_value),
        2
    ) AS total_usage,

    ROUND(
        AVG(usage_value),
        2
    ) AS average_usage

FROM hydroflow.benchmark.meter_readings_baseline

WHERE service_zone_id = 'zone_001'

  AND event_timestamp >=
      TIMESTAMP '2027-01-01 00:00:00'

  AND event_timestamp <
      TIMESTAMP '2027-04-01 00:00:00'

GROUP BY
    TO_DATE(event_timestamp),
    zone_name,
    meter_type

ORDER BY
    usage_date,
    meter_type;

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## Benchmark Query 2B - Dashboard-style Optimized

-- COMMAND ----------
SELECT
    TO_DATE(event_timestamp) AS usage_date,
    zone_name,
    meter_type,

    COUNT(*) AS reading_count,

    ROUND(
        SUM(usage_value),
        2
    ) AS total_usage,

    ROUND(
        AVG(usage_value),
        2
    ) AS average_usage

FROM hydroflow.benchmark.meter_readings_optimized

WHERE service_zone_id = 'zone_001'

  AND event_timestamp >=
      TIMESTAMP '2027-01-01 00:00:00'

  AND event_timestamp <
      TIMESTAMP '2027-04-01 00:00:00'

GROUP BY
    TO_DATE(event_timestamp),
    zone_name,
    meter_type

ORDER BY
    usage_date,
    meter_type;

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## Observed Query Profile Results
-- MAGIC
-- MAGIC ### Dashboard-style query: Baseline vs. Liquid Clustered
-- MAGIC
-- MAGIC Query Profile showed a clear difference in physical scan behavior for the
-- MAGIC identical zone + event-time analytical query.
-- MAGIC
-- MAGIC **Baseline (`meter_readings_baseline`)**
-- MAGIC - Rows read: 40,306
-- MAGIC - Files read: 20
-- MAGIC - Files pruned: 0
-- MAGIC - Bytes pruned: 0 B
-- MAGIC - Bytes read: approximately 13 MB
-- MAGIC
-- MAGIC **Liquid-clustered (`meter_readings_optimized`)**
-- MAGIC - Rows read: 40,306
-- MAGIC - Files read: 2
-- MAGIC - Files pruned: 16
-- MAGIC - Bytes pruned: 474.07 MB
-- MAGIC - Bytes read: approximately 0.9 MB
-- MAGIC
-- MAGIC Both queries read the same 40,306 qualifying rows, but the optimized table
-- MAGIC required substantially less physical I/O.
-- MAGIC
-- MAGIC The optimized table is clustered by `service_zone_id` and `event_timestamp`.
-- MAGIC This improved physical locality allows Delta file statistics to eliminate
-- MAGIC files whose value ranges cannot satisfy the query predicates.
-- MAGIC
-- MAGIC The baseline table was intentionally written with a scattered physical
-- MAGIC layout. Its file-level value ranges overlap the query predicates, so
-- MAGIC Databricks could not safely prune any of its 20 files.
-- MAGIC
-- MAGIC ### Benchmark interpretation
-- MAGIC
-- MAGIC Wall-clock duration is intentionally not used as the primary optimization
-- MAGIC metric in this benchmark.
-- MAGIC
-- MAGIC Repeated executions showed large runtime variation as the SQL Warehouse and
-- MAGIC its caches became warm. For example, a repeated baseline execution reported
-- MAGIC 100% of bytes read from cache even though it still read all 20 files and
-- MAGIC pruned 0 files.
-- MAGIC
-- MAGIC `SET use_cached_result = false` disables reuse of a previously computed query
-- MAGIC result, but it does not eliminate all lower-level caching and warm-execution
-- MAGIC effects.
-- MAGIC
-- MAGIC Therefore, the primary evidence for this benchmark is:
-- MAGIC
-- MAGIC - files read
-- MAGIC - files pruned
-- MAGIC - bytes read
-- MAGIC - bytes pruned
-- MAGIC - equivalent rows read
-- MAGIC
-- MAGIC These Query Profile metrics directly demonstrate the data-skipping benefit
-- MAGIC produced by Liquid Clustering, independent of unstable wall-clock timing.