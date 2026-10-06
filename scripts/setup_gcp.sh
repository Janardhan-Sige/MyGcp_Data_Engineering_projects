#!/bin/bash
# ============================================================
# Basic GCP setup helper.
# Run after installing the Google Cloud CLI and authenticating.
# Replace the placeholder values before executing.
# ============================================================

set -e

PROJECT_ID="YOUR_GCP_PROJECT_ID"
REGION="us-central1"
BUCKET="YOUR_GCS_BUCKET"

# Select the GCP project for subsequent gcloud commands.
gcloud config set project "$PROJECT_ID"

# Enable the APIs required by this project.
gcloud services enable   dataflow.googleapis.com   composer.googleapis.com   pubsub.googleapis.com   bigquery.googleapis.com   storage.googleapis.com

# Create the Cloud Storage bucket used by the demo.
gcloud storage buckets create "gs://$BUCKET"   --location="$REGION"

# Create the Pub/Sub topic used by the streaming example.
gcloud pubsub topics create orders-events

# Create a subscription so Dataflow can consume the topic.
gcloud pubsub subscriptions create orders-events-subscription   --topic=orders-events

echo "GCP foundation setup completed."
