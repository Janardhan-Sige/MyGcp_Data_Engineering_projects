"""Airflow DAG for deploying the long-running streaming Dataflow job.

Cloud Composer hosts the DAG and starts the Dataflow process. Pub/Sub then
continues supplying events independently of the Airflow schedule.
"""

from datetime import datetime

from airflow import DAG
from airflow.providers.google.cloud.operators.dataflow import DataflowCreatePythonJobOperator

PROJECT_ID = "{{ var.value.gcp_project_id }}"
REGION = "{{ var.value.gcp_region | default('us-central1') }}"

# The Beam source file is uploaded into the Composer DAGs directory.
DATAFLOW_PY_FILE = "/home/airflow/gcs/dags/dataflow/streaming/streaming_orders_pipeline.py"

with DAG(
    dag_id="streaming_orders_dataflow",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["gcp", "dataflow", "streaming", "pubsub"],
    description="Manually deploy the long-running streaming Dataflow job.",
) as dag:

    # A streaming pipeline is normally started once and then kept running.
    deploy_streaming_job = DataflowCreatePythonJobOperator(
        task_id="deploy_streaming_dataflow",
        project_id=PROJECT_ID,
        location=REGION,
        py_file=DATAFLOW_PY_FILE,
        options={
            "input_subscription": (
                f"projects/{PROJECT_ID}/subscriptions/orders-events-subscription"
            ),
            "output_table": f"{PROJECT_ID}:retail_dw.raw_order_events",
            "region": REGION,
        },
        job_name="streaming-orders-pipeline",
    )
