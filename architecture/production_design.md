# Production Design Notes

## 1. Idempotency

The batch path uses order_line_id as the business key and ranks source records by updated_at. A retry therefore selects the same winning version instead of blindly creating another fact row.

For streaming, event_id should be treated as the event identity. A production implementation can persist processed event IDs or use a downstream MERGE strategy where the business semantics require exactly-once effects.

## 2. Late-arriving events

Streaming uses event time rather than processing time. The production Beam example allows late data and defines early/late trigger behavior. This is important when mobile clients reconnect, networks delay messages or upstream services retry events.

## 3. Dead-letter strategy

Invalid messages are not printed and forgotten. They are emitted to a separate branch containing the original payload and failure reason. This creates a replay/debug path and allows valid traffic to continue.

## 4. SCD Type 2

Customer history is preserved using effective dates and is_current. This is useful for point-in-time reporting and is a common interview topic for warehouse design.

## 5. BigQuery optimization

The fact table is partitioned by business date and clustered by high-value access dimensions. The project avoids date-sharded tables and avoids SELECT * in analytical SQL. BigQuery documentation recommends partitioning for partition pruning and clustering for block pruning.

## 6. Operational observability

The pipeline audit model records:

- run_id
- pipeline_name
- execution_date
- status
- source_rows
- accepted_rows
- rejected_rows
- duplicate_rows
- target_table
- Dataflow job ID
- error message

A production engineer should be able to answer: What ran? When? How much data? How many failures? Which target? Which Dataflow job?

## 7. Backfill strategy

Because raw data is retained separately from marts, a historical date range can be reprocessed without rebuilding unrelated periods. Partitioned facts and date-scoped SQL reduce the blast radius of a backfill.

## 8. Failure scenarios to discuss in interviews

| Failure | Expected response |
|---|---|
| Dataflow worker failure | Dataflow retries failed work; monitor worker/backlog metrics |
| Composer task failure | Airflow retry policy + task-level observability |
| Duplicate landing file | Deterministic business-key dedupe |
| Bad source row | Quarantine with rule-level failure reason |
| Pub/Sub backlog grows | Scale Dataflow workers and inspect processing bottleneck |
| Late event | Event-time window + allowed lateness |
| Customer attribute changes | SCD Type 2 new version |
| BigQuery query scans too much | Partition filter + clustering-aware predicates |
| Partial downstream failure | Re-run idempotent stage rather than duplicating facts |

## Reference basis

The design follows current Google Cloud guidance for Dataflow, Pub/Sub-to-BigQuery streaming, BigQuery partitioning/clustering and Storage Write API usage. It also uses the public GoogleCloudPlatform Dataflow Cookbook and Dataflow Templates as implementation references.
