-- ============================================================
-- Clean the raw batch layer and create a trusted analytical layer.
-- This is SQL-based transformation after Dataflow ingestion.
-- ============================================================

CREATE OR REPLACE TABLE `YOUR_GCP_PROJECT_ID.retail_dw.clean_orders`
PARTITION BY DATE(order_ts)
CLUSTER BY customer_id, status AS
SELECT
  order_id,
  customer_id,
  order_ts,
  product_id,
  quantity,
  unit_price,
  UPPER(TRIM(status)) AS status,
  INITCAP(TRIM(city)) AS city,
  ROUND(quantity * unit_price, 2) AS order_amount
FROM `YOUR_GCP_PROJECT_ID.retail_dw.raw_orders`
WHERE quantity > 0
  AND unit_price >= 0
  AND order_id IS NOT NULL;
