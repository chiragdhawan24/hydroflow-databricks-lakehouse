-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 09 - Gold Materialized Views
-- MAGIC
-- MAGIC Run this file using Databricks SQL with a Pro or Serverless SQL Warehouse.
-- MAGIC
-- MAGIC Gold outputs:
-- MAGIC - gold_zone_usage_daily
-- MAGIC - gold_leak_risk_by_zone
-- MAGIC - gold_outage_sla
-- MAGIC - gold_customer_360
-- MAGIC - gold_data_quality_summary

-- COMMAND ----------
CREATE SCHEMA IF NOT EXISTS hydroflow.gold;

-- COMMAND ----------
-- Gold 1: Daily zone usage

CREATE OR REPLACE MATERIALIZED VIEW hydroflow.gold.gold_zone_usage_daily
TRIGGER ON UPDATE
AS
SELECT
    TO_DATE(event_timestamp) AS usage_date,
    service_zone_id,
    zone_name,
    zone_city,
    meter_type,
    COUNT(*) AS reading_count,
    COUNT(DISTINCT meter_id) AS meter_count,
    ROUND(SUM(usage_value), 2) AS total_usage,
    ROUND(AVG(usage_value), 2) AS average_usage,
    ROUND(AVG(pressure_psi), 2) AS average_pressure_psi
FROM hydroflow.silver.silver_meter_readings_zoned
GROUP BY
    TO_DATE(event_timestamp),
    service_zone_id,
    zone_name,
    zone_city,
    meter_type;

-- COMMAND ----------
-- Gold 2: Leak-risk analytics by zone
--
-- Project heuristic:
-- a water reading is considered suspicious when pressure < 40 PSI
-- or usage_value > 55 gallons.

CREATE OR REPLACE MATERIALIZED VIEW hydroflow.gold.gold_leak_risk_by_zone
TRIGGER ON UPDATE
AS
WITH zone_risk AS (
    SELECT
        TO_DATE(event_timestamp) AS usage_date,
        service_zone_id,
        zone_name,
        COUNT(*) AS water_readings,
        SUM(
            CASE
                WHEN pressure_psi < 40 OR usage_value > 55
                THEN 1
                ELSE 0
            END
        ) AS suspicious_readings,
        ROUND(
            100.0 * SUM(
                CASE
                    WHEN pressure_psi < 40 OR usage_value > 55
                    THEN 1
                    ELSE 0
                END
            ) / COUNT(*),
            2
        ) AS suspicious_rate_pct
    FROM hydroflow.silver.silver_meter_readings_zoned
    WHERE meter_type = 'water'
    GROUP BY
        TO_DATE(event_timestamp),
        service_zone_id,
        zone_name
)
SELECT
    usage_date,
    service_zone_id,
    zone_name,
    water_readings,
    suspicious_readings,
    suspicious_rate_pct,
    CASE
        WHEN suspicious_rate_pct >= 20 THEN 'high'
        WHEN suspicious_rate_pct >= 10 THEN 'medium'
        ELSE 'low'
    END AS leak_risk_level
FROM zone_risk;

-- COMMAND ----------
-- Gold 3: Outage SLA analytics

CREATE OR REPLACE MATERIALIZED VIEW hydroflow.gold.gold_outage_sla
TRIGGER ON UPDATE
AS
WITH outage_summary AS (
    SELECT
        outage_id,
        service_zone_id,
        zone_name,
        outage_event_type,
        outage_severity,
        outage_event_timestamp,
        resolved_timestamp,
        COUNT(DISTINCT meter_event_id) AS matched_meter_readings,
        TIMESTAMPDIFF(
            MINUTE,
            outage_event_timestamp,
            resolved_timestamp
        ) AS actual_duration_minutes,
        CASE
            WHEN outage_severity = 'critical' THEN 60
            WHEN outage_severity = 'high' THEN 120
            WHEN outage_severity = 'medium' THEN 180
            ELSE 240
        END AS sla_target_minutes
    FROM hydroflow.silver.silver_meter_outage_matches
    GROUP BY
        outage_id,
        service_zone_id,
        zone_name,
        outage_event_type,
        outage_severity,
        outage_event_timestamp,
        resolved_timestamp
)
SELECT
    outage_id,
    service_zone_id,
    zone_name,
    outage_event_type,
    outage_severity,
    outage_event_timestamp,
    resolved_timestamp,
    matched_meter_readings,
    actual_duration_minutes,
    sla_target_minutes,
    CASE
        WHEN resolved_timestamp IS NULL THEN FALSE
        WHEN actual_duration_minutes <= sla_target_minutes THEN TRUE
        ELSE FALSE
    END AS sla_met
FROM outage_summary;

-- COMMAND ----------
-- Gold 4: Customer 360

CREATE OR REPLACE MATERIALIZED VIEW hydroflow.gold.gold_customer_360
TRIGGER ON UPDATE
AS
WITH meter_summary AS (
    SELECT
        customer_id,
        COUNT(DISTINCT meter_id) AS meter_count,
        ROUND(
            SUM(
                CASE
                    WHEN meter_type = 'water'
                    THEN usage_value
                    ELSE 0
                END
            ),
            2
        ) AS water_usage_gallons,
        ROUND(
            SUM(
                CASE
                    WHEN meter_type = 'electric'
                    THEN usage_value
                    ELSE 0
                END
            ),
            2
        ) AS electric_usage_kwh
    FROM hydroflow.silver.silver_meter_readings_zoned
    GROUP BY customer_id
),
billing_summary AS (
    SELECT
        customer_id,
        ROUND(SUM(usage_gallons), 2) AS billed_water_usage_gallons,
        ROUND(SUM(bill_amount_usd), 2) AS total_bill_amount_usd,
        MAX_BY(payment_status, billing_month) AS latest_payment_status
    FROM hydroflow.silver.silver_billing
    GROUP BY customer_id
)
SELECT
    customer.customer_id,
    customer.customer_name,
    customer.segment,
    customer.service_zone_id,
    customer.account_status,
    COALESCE(meter.meter_count, 0) AS meter_count,
    COALESCE(meter.water_usage_gallons, 0) AS water_usage_gallons,
    COALESCE(meter.electric_usage_kwh, 0) AS electric_usage_kwh,
    COALESCE(
        billing.billed_water_usage_gallons,
        0
    ) AS billed_water_usage_gallons,
    COALESCE(
        billing.total_bill_amount_usd,
        0
    ) AS total_bill_amount_usd,
    billing.latest_payment_status
FROM hydroflow.silver.dim_customer_scd2 AS customer
LEFT JOIN meter_summary AS meter
    ON customer.customer_id = meter.customer_id
LEFT JOIN billing_summary AS billing
    ON customer.customer_id = billing.customer_id
WHERE customer.is_current = TRUE;

-- COMMAND ----------
-- Gold 5: Data-quality summary

CREATE OR REPLACE MATERIALIZED VIEW hydroflow.gold.gold_data_quality_summary
TRIGGER ON UPDATE
AS
SELECT
    quality.total_records,
    quality.valid_records,
    quality.invalid_records,
    quality.invalid_rate_pct,
    dedup.source_records,
    dedup.deduplicated_records,
    dedup.duplicates_removed,
    dedup.duplicate_rate_pct
FROM (
    SELECT
        MAX_BY(total_records, processed_at) AS total_records,
        MAX_BY(valid_records, processed_at) AS valid_records,
        MAX_BY(invalid_records, processed_at) AS invalid_records,
        MAX_BY(invalid_rate_pct, processed_at) AS invalid_rate_pct
    FROM hydroflow.silver.data_quality_metrics
) AS quality
CROSS JOIN (
    SELECT
        MAX_BY(source_records, measured_at) AS source_records,
        MAX_BY(deduplicated_records, measured_at) AS deduplicated_records,
        MAX_BY(duplicates_removed, measured_at) AS duplicates_removed,
        MAX_BY(duplicate_rate_pct, measured_at) AS duplicate_rate_pct
    FROM hydroflow.silver.deduplication_metrics
) AS dedup;

-- COMMAND ----------
-- Validation

SHOW TABLES IN hydroflow.gold;

-- COMMAND ----------
SELECT
    'gold_zone_usage_daily' AS gold_object,
    COUNT(*) AS row_count
FROM hydroflow.gold.gold_zone_usage_daily

UNION ALL

SELECT
    'gold_leak_risk_by_zone',
    COUNT(*)
FROM hydroflow.gold.gold_leak_risk_by_zone

UNION ALL

SELECT
    'gold_outage_sla',
    COUNT(*)
FROM hydroflow.gold.gold_outage_sla

UNION ALL

SELECT
    'gold_customer_360',
    COUNT(*)
FROM hydroflow.gold.gold_customer_360

UNION ALL

SELECT
    'gold_data_quality_summary',
    COUNT(*)
FROM hydroflow.gold.gold_data_quality_summary;
