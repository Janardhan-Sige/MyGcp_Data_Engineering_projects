# Medium Dataset

This folder contains a synthetic but deliberately medium-sized e-commerce workload.

| Dataset | Records | Format | Purpose |
|---|---:|---|---|
| orders_part_01..04 | 10,000 order-line rows | CSV | Batch ingestion |
| customers | 500 customers | CSV | Customer dimension / SCD2 |
| products | 120 products | CSV | Product dimension |
| stream_events_01..02 | 5,000 events | JSONL | Pub/Sub / Dataflow streaming |

The records are synthetic. They are not copied from a company database and contain no personal data.

The dataset intentionally includes discounts, tax, channels, payment methods, source systems, update timestamps and lifecycle status values so the SQL and pipeline logic resembles a production workload.

Use data/generate_medium_dataset.py to regenerate deterministic data locally.