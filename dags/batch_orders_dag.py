"""Airflow DAG for the batch order pipeline.

The DAG demonstrates orchestration rather than data transformation:
Airflow decides WHEN the Dataflow job should run and passes runtime parameters.
"""

from datetime import datetime

from airflow import DAG
from airflow.providers.google.cloud.operators.dataflow import DataflowCreatePythonJobOperator


PROJECT_ID = "{{ var.value.gcp_project_id }}"
REGION = "{{ var.value.gcp_region | default('us-central1') }}"
BUCKET = "{{ var.value.gcs_bucket }}"

with DAG(
    dag_id="batch_orders_dataflow",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["gcp", "dataflow", "batch"],
    description="Run the batch orders Dataflow pipeline once per day.",
) as dag:

    # Airflow launches the Python Beam pipeline as a managed Dataflow job.
    run_dataflow = DataflowCreatePythonJobOperator(
        task_id="run_batch_dataflow",
        project_id=PROJECT_ID,
        location=REGION,
        py_file=f"gs://{BUCKET}/dataflow/batch/batch_orders_pipeline.py",
        options={
            "input": f"gs://{BUCKET}/data/batch/orders.csv",
            "output_table": f"{PROJECT_ID}:retail_dw.raw_orders",
            "region": REGION,
        },
        job_name="batch-orders-{{ ds_nodash }}",
    )
