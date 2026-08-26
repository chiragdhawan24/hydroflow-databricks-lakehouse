-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 12 - Unity Catalog Governance
-- MAGIC
-- MAGIC Demonstrates fine-grained governance for HydroFlow using:
-- MAGIC
-- MAGIC - Unity Catalog privileges
-- MAGIC - Row-level filtering
-- MAGIC - Column masking
-- MAGIC - Governance metadata inspection
-- MAGIC
-- MAGIC Governance is applied to a dedicated demonstration table so that
-- MAGIC production-style Silver pipeline outputs remain unchanged.

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS hydroflow.governance;

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## Governance demonstration table

-- COMMAND ----------

CREATE OR REPLACE TABLE hydroflow.governance.customer_access_demo
USING DELTA
AS
SELECT
    customer_id,
    customer_name,
    email,
    phone,
    service_zone_id,
    account_status,
    effective_start_timestamp,
    effective_end_timestamp,
    is_current
FROM hydroflow.silver.dim_customer_scd2
WHERE is_current = true;

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## Column masking
-- MAGIC
-- MAGIC Mask customer email addresses at query time.

-- COMMAND ----------

CREATE OR REPLACE FUNCTION hydroflow.governance.mask_customer_email(
    email STRING
)
RETURN
    CASE
        WHEN email IS NULL THEN NULL
        ELSE regexp_replace(
            email,
            '^(.).+(@.+)$',
            '$1***$2'
        )
    END;

-- COMMAND ----------

ALTER TABLE hydroflow.governance.customer_access_demo
ALTER COLUMN email
SET MASK hydroflow.governance.mask_customer_email;

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## Row filtering
-- MAGIC
-- MAGIC Restrict the demonstration table to a controlled service-zone scope.
-- MAGIC
-- MAGIC In a production deployment, this function would normally use
-- MAGIC user/group identity or a principal-to-zone mapping table.

-- COMMAND ----------

CREATE OR REPLACE FUNCTION hydroflow.governance.filter_service_zone(
    service_zone_id STRING
)
RETURN service_zone_id IN ('zone_001', 'zone_002');

-- COMMAND ----------

ALTER TABLE hydroflow.governance.customer_access_demo
SET ROW FILTER hydroflow.governance.filter_service_zone
ON (service_zone_id);

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## Verify governed access

-- COMMAND ----------

SELECT *
FROM hydroflow.governance.customer_access_demo
ORDER BY customer_id;

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## Inspect Unity Catalog grants

-- COMMAND ----------

SHOW GRANTS ON CATALOG hydroflow;

-- COMMAND ----------

SHOW GRANTS ON SCHEMA hydroflow.governance;

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## Inspect row-filter metadata

-- COMMAND ----------

SELECT *
FROM hydroflow.information_schema.row_filters
WHERE table_schema = 'governance'
  AND table_name = 'customer_access_demo';

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## Inspect column-mask metadata

-- COMMAND ----------

SELECT *
FROM hydroflow.information_schema.column_masks
WHERE table_schema = 'governance'
  AND table_name = 'customer_access_demo';