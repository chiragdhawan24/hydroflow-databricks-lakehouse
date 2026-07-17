# Synthetic Raw Data Generator

The generator creates raw landing-zone files for the HydroFlow Lakehouse project.

## Data Sources

| Source | Format | Purpose |
|---|---|---|
| meter_readings | JSONL | Streaming smart-meter telemetry |
| outage_events | JSONL | Streaming outage/leak alerts |
| customer_cdc | JSONL | CDC feed for SCD Type 2 processing |
| billing | JSONL | Batch billing data for customer analytics |
| gis_zones | CSV | Static GIS/service-zone lookup |

## Quick Run

```bash
python data_generator/generate_raw_data.py --output data/raw_sample --customers 50 --meters 50 --days 2 --readings-per-meter-per-day 4
```

## Larger Run

```bash
python data_generator/generate_raw_data.py --output data/raw --customers 5000 --meters 5000 --days 3 --readings-per-meter-per-day 24
```

## Intentional Data Conditions

The generated data intentionally includes:

- Duplicate meter events
- Invalid readings such as negative usage, null meter IDs, and malformed status values
- CDC inserts, updates, and deletes
- Static service zones
- Event timestamps suitable for streaming watermarks

These conditions support downstream Databricks implementations for quality enforcement, deduplication, CDC, SCD Type 2, streaming joins, and Gold-layer metrics.
