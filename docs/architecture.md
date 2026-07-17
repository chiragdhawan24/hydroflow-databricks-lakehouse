# HydroFlow Architecture

HydroFlow is built around a Medallion Architecture.

## Bronze Layer

The Bronze layer stores raw files and raw Delta tables. It preserves the original data with minimal transformation and adds ingestion metadata.

Planned tables:

- `bronze_meter_readings`
- `bronze_outage_events`
- `bronze_customer_cdc`
- `bronze_billing`
- `bronze_raw_events` for multiplex ingestion

## Silver Layer

The Silver layer applies data quality rules, deduplication, schema normalization, CDC processing, and joins.

Planned tables:

- `silver_meter_readings_clean`
- `silver_meter_readings_quarantine`
- `silver_customer_dim_scd2`
- `silver_outage_events_clean`
- `silver_enriched_meter_events`

## Gold Layer

The Gold layer creates analytics-ready tables for dashboards and reporting.

Planned tables:

- `gold_zone_usage_daily`
- `gold_leak_risk_by_zone`
- `gold_outage_sla`
- `gold_customer_360`
- `gold_data_quality_metrics`

## Key Engineering Concepts

- Auto Loader for incremental ingestion
- Multiplex Bronze table for raw event standardization
- Delta Lake transaction log and Change Data Feed
- Structured Streaming for near-real-time processing
- Stream-static and stream-stream joins
- SCD Type 2 customer/device history
- Gold table optimization and benchmarking
- Lakeflow Jobs orchestration
- Lakeflow Spark Declarative Pipelines
- Unity Catalog governance
- Declarative Automation Bundles deployment
