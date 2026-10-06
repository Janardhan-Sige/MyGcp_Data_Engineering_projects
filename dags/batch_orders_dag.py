"""Airflow DAG for the batch order Dataflow pipeline.

The DAG demonstrates orchestration: Airflow decides WHEN the Dataflow job
runs, while Dataflow performs the distributed data processing.

Before deploying to Cloud Composer, upload the repository's dataflow/ folder
under the Composer DAGs folder so the Python Beam files are available locally.
"""

from datetime import datetime

from airflow import DAG
from airflow.providers.google.cloud.operators.dataflow import DataflowCreatePythonJobOperator

PROJECT_ID = "{{ var.value.gcp_project_id }}"
REGION = "{{ var.value.gcp_region | default('us-central1') }}"
BUCKET = "{{ var.value.gcs_bucket }}"

# Composer exposes its DAG bucket through /home/airflow/gcs/dags.
# DataflowCreatePythonJobOperator expects the Python entrypoint to be
# accessible from the Composer worker, so we use that local Composer path.
DATAFLOW_PY_FILE = "/home/airflow/gcs/dags/dataflow/batch/batch_orders_pipeline.py"

with DAG(
    dag_id="batch_orders_dataflow",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["gcp", "dataflow", "batch"],
    description="Run the batch orders Dataflow pipeline once per day.",
) as dag:

    run_dataflow = DataflowCreatePythonJobOperator(
        task_id="run_batch_dataflow",
        project_id=PROJECT_ID,
        location=REGION,
        py_file=DATAFLOW_PY_FILE,
        options={
            "input": f"gs://{BUCKET}/data/batch/orders.csv",
            "output_table": f"{PROJECT_ID}:retail_dw.raw_orders",
            "region": REGION,
        },
        job_name="batch-orders-{{ ds_nodash }}",
    )
