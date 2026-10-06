#!/bin/bash
# ============================================================
# Upload local project assets to the Cloud Storage bucket.
# Replace YOUR_GCS_BUCKET before running.
# ============================================================

set -e

BUCKET="YOUR_GCS_BUCKET"

# Upload sample batch data.
gcloud storage cp data/batch/orders.csv "gs://$BUCKET/data/batch/orders.csv"

# Upload the Dataflow source code.
gcloud storage cp dataflow/batch/batch_orders_pipeline.py   "gs://$BUCKET/dataflow/batch/batch_orders_pipeline.py"

gcloud storage cp dataflow/streaming/streaming_orders_pipeline.py   "gs://$BUCKET/dataflow/streaming/streaming_orders_pipeline.py"

echo "Project assets uploaded."
