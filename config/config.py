"""Central configuration for the GCP learning project.

Keep environment-specific values here so the pipeline code stays reusable.
For real deployments, prefer Airflow Variables, Secret Manager, or runtime
pipeline options instead of hard-coding credentials.
"""

import os

# GCP project ID is supplied at runtime.
PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "YOUR_GCP_PROJECT_ID")

# Region used by Dataflow and other regional services.
REGION = os.environ.get("GCP_REGION", "us-central1")

# Cloud Storage bucket used for batch input and Dataflow staging.
BUCKET = os.environ.get("GCS_BUCKET", "YOUR_GCS_BUCKET")

# Pub/Sub topic and subscription used by the streaming example.
PUBSUB_TOPIC = os.environ.get("PUBSUB_TOPIC", "orders-events")
PUBSUB_SUBSCRIPTION = os.environ.get(
    "PUBSUB_SUBSCRIPTION", "orders-events-subscription"
)

# BigQuery dataset used by the project.
BQ_DATASET = os.environ.get("BQ_DATASET", "retail_dw")
