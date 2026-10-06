-- Deterministic deduplication.
-- Production pipelines should not assume the source sends each business key once.
-- Keep the newest record per order_line_id based on updated_at and ingestion_ts.

CREATE OR REPLACE TABLE `YOUR_GCP_PROJECT_ID.retail_stg.stg_order_lines_deduped`
PARTITION BY DATE(order_ts)
CLUSTER BY customer_id, product_id
AS
WITH ranked AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY order_line_id
      ORDER BY updated_at DESC, ingestion_ts DESC
    ) AS rn
  FROM `YOUR_GCP_PROJECT_ID.retail_raw.raw_order_lines`
)
SELECT
  order_id,
  order_line_id,
  customer_id,
  TIMESTAMP(order_ts) AS order_ts,
  product_id,
  quantity,
  CAST(unit_price AS NUMERIC) AS unit_price,
  CAST(discount_amount AS NUMERIC) AS discount_amount,
  CAST(tax_amount AS NUMERIC) AS tax_amount,
  CAST(gross_amount AS NUMERIC) AS gross_amount,
  CAST(net_amount AS NUMERIC) AS net_amount,
  UPPER(status) AS status,
  UPPER(payment_method) AS payment_method,
  UPPER(channel) AS channel,
  INITCAP(city) AS city,
  updated_at,
  source_system,
  ingestion_ts
FROM ranked
WHERE rn = 1;
