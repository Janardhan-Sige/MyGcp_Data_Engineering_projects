"""Production-style Composer DAG for the retail batch platform.

Airflow is responsible for orchestration and control flow. SQL and Dataflow
perform the actual data processing. Each stage is intentionally observable
and independently retryable.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.dataflow import DataflowCreatePythonJobOperator

PROJECT_ID = "{{ var.value.gcp_project_id }}"
REGION = "{{ var.value.gcp_region | default('us-central1') }}"
BUCKET = "{{ var.value.gcs_bucket }}"

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="retail_orders_daily_production",
    start_date=datetime(2026, 1, 1),
    schedule="0 2 * * *",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["retail", "batch", "dataflow", "bigquery", "production-pattern"],
) as dag:

    start = EmptyOperator(task_id="start")

    ingest = DataflowCreatePythonJobOperator(
        task_id="dataflow_ingest_orders",
        project_id=PROJECT_ID,
        location=REGION,
        py_file="/home/airflow/gcs/dags/dataflow/batch/batch_orders_pipeline.py",
        options={
            "input": f"gs://{BUCKET}/data/medium/orders_part_*.csv",
            "output_table": f"{PROJECT_ID}:retail_raw.raw_order_lines",
            "region": REGION,
        },
        job_name="retail-batch-{{ ds_nodash }}",
    )

    dedupe = BigQueryInsertJobOperator(
        task_id="deduplicate_orders",
        configuration={
            "query": {
                "query": "{% include 'sql/transformations/deduplicate_orders.sql' %}",
                "useLegacySql": False,
            }
        },
    )

    dq = BigQueryInsertJobOperator(
        task_id="data_quality_gate",
        configuration={
            "query": {
                "query": "{% include 'sql/transformations/data_quality_orders.sql' %}",
                "useLegacySql": False,
            }
        },
    )

    scd2 = BigQueryInsertJobOperator(
        task_id="customer_scd2",
        configuration={
            "query": {
                "query": "{% include 'sql/dimensions/customer_scd2_merge.sql' %}",
                "useLegacySql": False,
            }
        },
    )

    marts = BigQueryInsertJobOperator(
        task_id="build_customer_and_kpi_marts",
        configuration={
            "query": {
                "query": "{% include 'sql/marts/customer_360.sql' %}",
                "useLegacySql": False,
            }
        },
    )

    audit = BigQueryInsertJobOperator(
        task_id="publish_pipeline_audit",
        configuration={
            "query": {
                "query": "{% include 'sql/operations/pipeline_audit.sql' %}",
                "useLegacySql": False,
            }
        },
    )

    finish = EmptyOperator(task_id="finish")

    start >> ingest >> dedupe >> dq >> scd2 >> marts >> audit >> finish
