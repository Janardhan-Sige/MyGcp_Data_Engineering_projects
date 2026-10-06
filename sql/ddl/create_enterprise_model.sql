-- Enterprise-style BigQuery model.
-- Layering:
--   raw_*      = immutable ingestion
--   stg_*      = typed, validated, deduplicated
--   dim_*      = conformed dimensions
--   fct_*      = business facts
--   mart_*     = analytics-facing aggregates
-- This keeps ingestion concerns separate from business logic.

CREATE SCHEMA IF NOT EXISTS `YOUR_GCP_PROJECT_ID.retail_raw`;
CREATE SCHEMA IF NOT EXISTS `YOUR_GCP_PROJECT_ID.retail_stg`;
CREATE SCHEMA IF NOT EXISTS `YOUR_GCP_PROJECT_ID.retail_dw`;
CREATE SCHEMA IF NOT EXISTS `YOUR_GCP_PROJECT_ID.retail_mart`;
CREATE SCHEMA IF NOT EXISTS `YOUR_GCP_PROJECT_ID.retail_ops`;

CREATE TABLE IF NOT EXISTS `YOUR_GCP_PROJECT_ID.retail_dw.dim_customer` (
  customer_sk INT64 NOT NULL,
  customer_id STRING NOT NULL,
  customer_name STRING,
  email STRING,
  segment STRING,
  city STRING,
  state STRING,
  effective_from TIMESTAMP NOT NULL,
  effective_to TIMESTAMP,
  is_current BOOL NOT NULL,
  record_hash STRING NOT NULL,
  source_system STRING,
  ingestion_ts TIMESTAMP NOT NULL
)
CLUSTER BY customer_id, is_current;

CREATE TABLE IF NOT EXISTS `YOUR_GCP_PROJECT_ID.retail_dw.dim_product` (
  product_sk INT64 NOT NULL,
  product_id STRING NOT NULL,
  product_name STRING,
  category STRING,
  brand STRING,
  unit_cost NUMERIC,
  list_price NUMERIC,
  active_flag BOOL,
  effective_from TIMESTAMP NOT NULL,
  effective_to TIMESTAMP,
  is_current BOOL NOT NULL,
  record_hash STRING NOT NULL,
  ingestion_ts TIMESTAMP NOT NULL
)
CLUSTER BY product_id, category;

CREATE TABLE IF NOT EXISTS `YOUR_GCP_PROJECT_ID.retail_dw.fct_order_line` (
  order_id STRING NOT NULL,
  order_line_id STRING NOT NULL,
  customer_id STRING NOT NULL,
  product_id STRING NOT NULL,
  order_ts TIMESTAMP NOT NULL,
  quantity INT64 NOT NULL,
  unit_price NUMERIC NOT NULL,
  discount_amount NUMERIC,
  tax_amount NUMERIC,
  gross_amount NUMERIC,
  net_amount NUMERIC,
  status STRING,
  payment_method STRING,
  channel STRING,
  city STRING,
  source_system STRING,
  ingestion_ts TIMESTAMP NOT NULL,
  batch_id STRING NOT NULL
)
PARTITION BY DATE(order_ts)
CLUSTER BY customer_id, product_id, status;
