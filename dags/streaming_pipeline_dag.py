"""Airflow DAG for deploying the streaming Dataflow pipeline.

A streaming job is long-running. The DAG therefore demonstrates deployment and
monitoring of the streaming pipeline rather than scheduling every event.
"""

from datetime import datetime

from airflow import DAG
from airflow.providers.google.cloud.operators.dataflow import DataflowCreatePythonJobOperator


PROJECT_ID = "{{ var.value.gcp_project_id }}"
REGION = "{{ var.value.gcp_region | default('us-central1') }}"
BUCKET = "{{ var.value.gcs_bucket }}"

with DAG(
    dag_id="streaming_orders_dataflow",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["gcp", "dataflow", "streaming", "pubsub"],
    description="Manually deploy the long-running streaming Dataflow job.",
) as dag:

    # schedule=None means an engineer can deploy the stream when required.
    # Pub/Sub keeps producing events while Dataflow continuously processes them.
    deploy_streaming_job = DataflowCreatePythonJobOperator(
        task_id="deploy_streaming_dataflow",
        project_id=PROJECT_ID,
        location=REGION,
        py_file=f"gs://{BUCKET}/dataflow/streaming/streaming_orders_pipeline.py",
        options={
            "input_subscription": (
                f"projects/{PROJECT_ID}/subscriptions/orders-events-subscription"
            ),
            "output_table": f"{PROJECT_ID}:retail_dw.raw_order_events",
            "region": REGION,
        },
        job_name="streaming-orders-pipeline",
    )
