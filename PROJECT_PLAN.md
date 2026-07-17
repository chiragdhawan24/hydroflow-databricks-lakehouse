# HydroFlow Implementation Plan

This plan keeps the project build manageable and GitHub-ready. Each step should create runnable code, a short README update, and a measurable output.

## Step 1 — Repository Foundation and Synthetic Data Generator

Goal: Create realistic raw utility data that can feed all downstream Databricks concepts.

Deliverables:
- GitHub-ready repository structure
- Synthetic data generator
- Raw data contracts
- Starter Bronze notebook skeleton
- Metrics template

Validation:
- Generated raw files exist under meter readings, outage events, customer CDC, billing, and GIS zones
- Duplicate meter events are present
- Invalid meter readings are present
- Customer CDC contains insert, update, and delete operations

## Step 2 — Bronze Auto Loader Ingestion

Goal: Incrementally ingest raw files using Databricks Auto Loader.

Deliverables:
- Auto Loader notebook for meter readings
- Checkpoint and schema locations
- Bronze Delta table
- Metadata columns: source file, ingestion timestamp, source system

## Step 3 — Multiplex Bronze

Goal: Store multiple raw entities in one normalized Bronze table.

Deliverables:
- bronze_raw_events table
- source_entity column
- raw_payload column
- schema_version column

## Step 4 — Silver Quality Enforcement

Goal: Clean and quarantine bad records.

Deliverables:
- silver_meter_readings table
- quarantine_invalid_meter_readings table
- Quality metrics table

## Step 5 — Streaming Deduplication

Goal: Remove duplicate event IDs in streaming data.

Deliverables:
- Deduplicated Silver stream
- Duplicate count metric

## Step 6 — CDC and SCD Type 2

Goal: Track customer/device history over time.

Deliverables:
- customer_dim_scd2
- device_dim_scd2
- current flag and effective date columns

## Step 7 — Delta Change Data Feed

Goal: Propagate changed Silver rows into Gold tables.

Deliverables:
- CDF enabled Delta table
- Incremental Gold update logic

## Step 8 — Joins

Goal: Enrich events with service zones and outage context.

Deliverables:
- Stream-static join: meter readings + GIS zones
- Stream-stream join: meter readings + outage events

## Step 9 — Gold Analytics

Goal: Build BI-ready tables.

Deliverables:
- gold_zone_usage_daily
- gold_leak_risk_by_zone
- gold_outage_sla
- gold_customer_360

## Step 10 — Optimization Benchmarks

Goal: Measure before/after query performance.

Deliverables:
- Baseline query profile
- Optimized query profile
- Partitioning and compaction notes

## Step 11 — Lakeflow Jobs and Pipelines

Goal: Orchestrate the full workflow.

Deliverables:
- Lakeflow Job config
- Declarative pipeline files
- Failure troubleshooting notes

## Step 12 — Governance and Deployment

Goal: Add production-style governance and deployment.

Deliverables:
- Unity Catalog SQL
- Row filters and column masks
- Declarative Automation Bundle config
- REST API trigger script
- CLI deployment notes
