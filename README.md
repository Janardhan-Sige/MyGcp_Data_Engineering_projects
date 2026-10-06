# GCP Data Engineering — 4-Year Experience Interview Project

> Production-style e-commerce data platform built with Python, SQL and Google Cloud.
>
> This repository is intentionally **not a beginner tutorial**. It is designed to demonstrate the decisions, failure handling, data modeling and operational thinking expected from a Data Engineer with around 4 years of experience.

## Business scenario

Assume an e-commerce company receives orders from web, mobile, marketplace and physical-store channels.

The platform must support:

- Daily batch ingestion of order-line data
- Near-real-time order lifecycle events
- Data quality and quarantine
- Deterministic deduplication
- Slowly Changing Dimension Type 2 for customer history
- Partitioned and clustered BigQuery warehouse tables
- Incremental transformations
- Auditability and operational metrics
- Analytics-ready marts
- Replayable streaming events
- Airflow/Cloud Composer orchestration
- Dataflow batch and streaming processing
- SQL-first warehouse transformations

## Architecture

![Enterprise architecture](docs/screenshots/end_to_end_architecture.svg)

### Batch

Cloud Storage
→ Cloud Composer
→ Dataflow batch
→ retail_raw
→ dedupe + DQ
→ retail_dw
→ retail_mart
→ BI / analytics

### Streaming

Application events
→ Pub/Sub
→ Dataflow streaming
→ event-time processing
→ BigQuery Storage Write API
→ retail_raw
→ streaming KPI marts

Google's current Dataflow guidance recommends the Storage Write API for modern streaming-to-BigQuery workloads, while batch workloads can use file loads depending on scale and workload characteristics. This project is structured to demonstrate those production decisions rather than relying on legacy streaming inserts.

## Realistic screenshots

### BigQuery warehouse table

![BigQuery table preview](docs/screenshots/bigquery_table_preview.svg)

The screenshot-style view shows the type of table a reviewer should expect to see in BigQuery: typed columns, realistic values, partitioning, clustering and business fields.

### Dataflow streaming job

![Dataflow monitoring](docs/screenshots/dataflow_job_monitor.svg)

The monitoring view demonstrates the operational metrics that matter in an interview: worker count, throughput, backlog, late data, DLQ rate and pipeline stages.

### Cloud Composer DAG

![Composer DAG](docs/screenshots/composer_dag.svg)

The DAG demonstrates dependency-driven orchestration rather than a single task that simply starts Dataflow.

> These are documentation screenshots/console-style visualizations created for this repository. The metrics shown are illustrative; they are not screenshots from a live production GCP account.

## Dataset scale

This repository now uses a **medium-sized synthetic workload**, not a six-row demo.

| Dataset | Volume | Format | Role |
|---|---:|---|---|
| Orders | 10,000 order-line records | CSV | Batch fact ingestion |
| Customers | 500 records | CSV | SCD Type 2 dimension |
| Products | 120 records | CSV | Conformed product dimension |
| Streaming events | 5,000 events | JSONL | Pub/Sub replay workload |
| Order files | 4 partitions | CSV | Simulates multiple landing objects |
| Event files | 2 partitions | JSONL | Simulates replay/chunked event delivery |

All data is synthetic and deterministic. It is safe to publish and is designed to behave like an e-commerce workload.

Dataset documentation: data/README.md  
Data dictionary: docs/data_dictionary.md  
Generator: data/generate_medium_dataset.py

## Warehouse design

The project uses explicit layers instead of putting every transformation into one BigQuery table.

| Layer | Example | Responsibility |
|---|---|---|
| RAW | retail_raw.raw_order_lines | Immutable source landing |
| STG | retail_stg.stg_order_lines_deduped | Typing, standardization, dedupe |
| DW | retail_dw.dim_customer | Historical customer state |
| DW | retail_dw.dim_product | Product master |
| DW | retail_dw.fct_order_line | Transaction fact |
| MART | retail_mart.customer_360 | Customer analytics |
| MART | retail_mart.daily_commercial_kpis | Executive KPIs |
| OPS | retail_ops.pipeline_run_audit | Pipeline observability |
| OPS | retail_ops.rejected_order_lines | Data-quality quarantine |

This separation makes the pipeline easier to backfill, troubleshoot and explain during an interview.

## Data quality strategy

Bad data should not simply disappear.

The project demonstrates checks for:

- Missing business keys
- Invalid quantity
- Negative price
- Invalid status
- Duplicate order-line keys
- Standardized status/channel/city values
- Source-to-target row-count reconciliation
- Rejected-row monitoring

Failed records are written to a quarantine table with a failure reason. The pipeline can then continue for valid records while the rejected population is investigated.

File: sql/transformations/data_quality_orders.sql

## Deduplication

The staging layer uses ROW_NUMBER over the business key:

- Partition by order_line_id
- Order by source updated_at
- Use ingestion timestamp as a deterministic tie-breaker

This is important for replayed files, retried jobs and late source updates.

File: sql/transformations/deduplicate_orders.sql

## SCD Type 2

Customer attributes are modeled historically.

Tracked versions use:

- effective_from
- effective_to
- is_current
- record_hash

This supports point-in-time analysis instead of overwriting customer history.

File: sql/dimensions/customer_scd2_merge.sql

## BigQuery performance design

Large analytical tables are partitioned and clustered based on access patterns.

The fact table is:

- Partitioned by order date
- Clustered by customer_id, product_id and status

The marts use business-date partitioning where appropriate.

The goal is to support partition pruning and block pruning rather than relying on SELECT * queries across the entire warehouse.

Google's documentation recommends partitioning when queries can benefit from scanning only relevant partitions and clustering when filters/aggregations repeatedly use high-cardinality columns.

## Batch pipeline

Main code:

dataflow/batch/batch_orders_pipeline.py

Production flow:

1. Detect/land multiple Cloud Storage files
2. Read bounded data with Apache Beam
3. Parse and type source fields
4. Apply validation
5. Write raw/accepted data
6. Run SQL deduplication
7. Run data-quality gate
8. Merge dimensions
9. Build fact table
10. Refresh analytical marts
11. Record pipeline audit metrics

Composer DAG:

dags/batch_orders_dag.py

## Streaming pipeline

Main code:

dataflow/streaming/streaming_orders_pipeline.py

Production flow:

1. Application publishes JSON events
2. Pub/Sub buffers and decouples producers/consumers
3. Dataflow consumes the subscription
4. Parse and validate events
5. Separate invalid records to DLQ/quarantine
6. Apply event-time windowing
7. Write to BigQuery
8. Build hourly/event KPIs
9. Monitor backlog and late-data behavior

Google documents the Pub/Sub → Dataflow → BigQuery pattern as a standard streaming architecture. The repository extends that pattern with operational controls and warehouse modeling.

## SQL portfolio

### DDL

- sql/ddl/create_datasets.sql
- sql/ddl/create_tables.sql
- sql/ddl/create_enterprise_model.sql

### Transformations

- sql/transformations/clean_orders.sql
- sql/transformations/deduplicate_orders.sql
- sql/transformations/data_quality_orders.sql
- sql/transformations/daily_sales.sql

### Dimensions

- sql/dimensions/customer_scd2_merge.sql

### Analytics marts

- sql/marts/customer_360.sql
- sql/marts/daily_commercial_kpis.sql
- sql/streaming/sessionized_orders.sql

### Operations

- sql/operations/pipeline_audit.sql

## Airflow / Cloud Composer

The orchestration layer should own workflow dependencies, retries, scheduling, quality gates and operational sequencing.

It should not perform distributed row-level processing itself.

Typical dependency chain:

detect files
→ start Dataflow
→ validate landing
→ deduplicate
→ DQ gate
→ SCD2 merge
→ fact build
→ marts
→ audit
→ notification

This demonstrates the distinction between:

**Airflow = orchestration**

**Dataflow = distributed processing**

**BigQuery = analytical warehouse**

## Python design

Python is used for:

- Apache Beam pipelines
- Pub/Sub producer
- Configuration
- Dataset generation
- Reusable transformations
- Unit tests

SQL is used for:

- Warehouse DDL
- Deduplication
- Data quality
- SCD Type 2
- Fact construction
- Analytics marts
- Operational reporting

No Spark or Java is required for the core project.

## Interview-level scenarios covered

| Scenario | Where to study |
|---|---|
| Duplicate files arrive twice | deduplicate_orders.sql |
| Source sends an invalid row | data_quality_orders.sql |
| Customer changes segment | customer_scd2_merge.sql |
| Streaming backlog increases | Dataflow screenshot + architecture |
| Late streaming events | streaming pipeline / windowing |
| Dataflow job fails midway | audit + retry/idempotency design |
| BigQuery query becomes expensive | partitioning + clustering |
| Need historical customer state | SCD2 |
| Need to replay events | JSONL medium dataset + producer |
| Need to prove pipeline health | pipeline_run_audit |
| Need business-facing metrics | daily_commercial_kpis |
| Need customer-level analytics | customer_360 |

## Recommended interview discussion

A strong explanation is:

> I designed a GCP e-commerce platform that handles both bounded and unbounded workloads. Cloud Storage provides the batch landing zone, Pub/Sub decouples streaming producers, Cloud Composer orchestrates dependencies, Dataflow performs distributed Python/Beam processing, and BigQuery provides the warehouse. I separated raw, staging, warehouse, mart and operational layers. The pipeline includes deterministic deduplication, data-quality quarantine, SCD Type 2 customer history, partitioning and clustering, streaming event-time processing, and audit metrics. The workload is represented by 10,000 order-line records and 5,000 streaming events so the design can be tested against something more realistic than a handful of rows.

## Repository structure

    MyGcp_Data_Engineering_projects/
    ├── architecture/
    ├── config/
    ├── dags/
    ├── data/
    │   ├── batch/
    │   ├── medium/
    │   └── streaming/
    ├── dataflow/
    │   ├── batch/
    │   ├── common/
    │   └── streaming/
    ├── docs/
    │   ├── data_dictionary.md
    │   └── screenshots/
    ├── producer/
    ├── scripts/
    ├── sql/
    │   ├── analytics/
    │   ├── ddl/
    │   ├── dimensions/
    │   ├── marts/
    │   ├── operations/
    │   ├── streaming/
    │   └── transformations/
    └── tests/

## Reference material

The architecture and implementation patterns were informed by:

- Google Cloud Dataflow documentation
- Google Cloud BigQuery documentation
- Google Cloud Pub/Sub → BigQuery guidance
- GoogleCloudPlatform Dataflow Cookbook
- GoogleCloudPlatform Dataflow Templates
- Public GCP data-engineering examples used only as architectural references

Official references:

https://cloud.google.com/dataflow/docs  
https://cloud.google.com/bigquery/docs  
https://cloud.google.com/pubsub/docs  
https://github.com/GoogleCloudPlatform/dataflow-cookbook  
https://github.com/GoogleCloudPlatform/DataflowTemplates

The sample records themselves are **original synthetic data generated for this repository**.

## Important

Replace all YOUR_GCP_PROJECT_ID and YOUR_GCS_BUCKET placeholders before deployment.

Configure IAM using least privilege.

Never commit service-account keys, OAuth tokens or other secrets.

This repository is an interview/portfolio project. The console-style screenshots are illustrative documentation, not claims of production execution.
