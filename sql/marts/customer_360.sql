-- Customer 360 mart.
-- Combines the current customer dimension with transactional behavior.
-- This is the type of table consumed by BI, CRM and segmentation use cases.

CREATE OR REPLACE TABLE `YOUR_GCP_PROJECT_ID.retail_mart.customer_360`
CLUSTER BY customer_id
AS
WITH sales AS (
  SELECT
    customer_id,
    COUNT(DISTINCT order_id) AS orders,
    SUM(IF(status='COMPLETED', net_amount, 0)) AS completed_revenue,
    SUM(quantity) AS units,
    MAX(order_ts) AS last_order_ts
  FROM `YOUR_GCP_PROJECT_ID.retail_dw.fct_order_line`
  GROUP BY customer_id
)
SELECT
  c.customer_id,
  c.customer_name,
  c.segment,
  c.city,
  c.state,
  COALESCE(s.orders,0) AS orders,
  ROUND(COALESCE(s.completed_revenue,0),2) AS completed_revenue,
  COALESCE(s.units,0) AS units,
  s.last_order_ts,
  CASE
    WHEN COALESCE(s.completed_revenue,0) >= 5000 THEN 'HIGH_VALUE'
    WHEN COALESCE(s.completed_revenue,0) >= 1500 THEN 'GROWTH'
    ELSE 'STANDARD'
  END AS value_segment
FROM `YOUR_GCP_PROJECT_ID.retail_dw.dim_customer` c
LEFT JOIN sales s USING(customer_id)
WHERE c.is_current = TRUE;
