-- ============================================================
-- Create the raw batch and streaming tables.
-- These tables represent the landing layer before SQL analytics.
-- ============================================================

CREATE TABLE IF NOT EXISTS `YOUR_GCP_PROJECT_ID.retail_dw.raw_orders` (
  order_id STRING NOT NULL,
  customer_id STRING NOT NULL,
  order_ts DATETIME NOT NULL,
  product_id STRING NOT NULL,
  quantity INT64,
  unit_price FLOAT64,
  status STRING,
  city STRING,
  order_amount FLOAT64
)
PARTITION BY DATE(order_ts)
CLUSTER BY customer_id, status;

CREATE TABLE IF NOT EXISTS `YOUR_GCP_PROJECT_ID.retail_dw.raw_order_events` (
  order_id STRING,
  customer_id STRING,
  product_id STRING,
  quantity INT64,
  unit_price FLOAT64,
  order_amount FLOAT64,
  event_type STRING,
  event_ts TIMESTAMP,
  city STRING
)
PARTITION BY DATE(event_ts)
CLUSTER BY customer_id, event_type;
