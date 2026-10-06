# Architecture and Interview Notes

## 1. Batch path

Cloud Storage stores a CSV file. Cloud Composer schedules the batch DAG.
The DAG launches a Python Apache Beam job on Dataflow. Dataflow parses and
cleans each record and writes the result to BigQuery.

## 2. Streaming path

A Python producer publishes JSON order events to Pub/Sub. Dataflow reads the
unbounded Pub/Sub subscription, decodes each message, calculates order amount,
and writes the event to BigQuery.

## 3. Why Airflow?

Airflow is the orchestration layer. It answers **when**, **what depends on what**,
and **what should happen after a failure**. Dataflow is the processing engine.

## 4. Why Dataflow?

Dataflow provides managed execution for Apache Beam pipelines. The same Beam
programming model can express batch and streaming transformations.

## 5. Why BigQuery?

BigQuery is the analytical warehouse in this project. SQL is used after
ingestion for trusted tables, KPIs, customer analytics, and revenue analysis.

## Interview questions to practice

- Why use Dataflow instead of doing all transformations in Airflow?
- What is a PCollection?
- What is the difference between bounded and unbounded data?
- How does Pub/Sub support streaming?
- How would you handle bad records?
- How would you make a pipeline idempotent?
- What is the difference between partitioning and clustering in BigQuery?
- How would you monitor a failed Dataflow job?
- How would you design a dead-letter queue?
- How would you optimize a slow BigQuery query?
