# HydroFlow Metrics Template

Fill this file after implementing each benchmark.

## Raw Data Generation

| Metric | Value |
|---|---:|
| Meter events generated | TBD |
| Outage events generated | TBD |
| Customer CDC events generated | TBD |
| Billing records generated | TBD |
| GIS zones generated | TBD |

## Data Quality

| Metric | Value |
|---|---:|
| Invalid records detected | TBD |
| Invalid records quarantined | TBD |
| Quarantine accuracy | TBD |
| Duplicate events detected | TBD |
| Duplicate events removed | TBD |

## Pipeline Latency

| Stage | Latency |
|---|---:|
| Raw landing to Bronze | TBD |
| Bronze to Silver | TBD |
| Silver to Gold | TBD |
| End-to-end Bronze to Gold | TBD |

## Gold Query Benchmark

| Query | Baseline Runtime | Optimized Runtime | Improvement |
|---|---:|---:|---:|
| Zone usage daily | TBD | TBD | TBD |
| Leak risk by zone | TBD | TBD | TBD |
| Customer 360 | TBD | TBD | TBD |

## Optimization Techniques Applied

- [ ] Partitioning
- [ ] File compaction
- [ ] OPTIMIZE
- [ ] Query Profile tuning
- [ ] Data skipping / layout improvements
- [ ] CDF-based incremental processing
