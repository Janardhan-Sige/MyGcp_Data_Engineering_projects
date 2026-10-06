-- Executive KPI mart.
-- Designed for partition pruning and dashboard refreshes.

CREATE OR REPLACE TABLE `YOUR_GCP_PROJECT_ID.retail_mart.daily_commercial_kpis`
PARTITION BY business_date
CLUSTER BY channel, city
AS
SELECT
  DATE(order_ts) AS business_date,
  channel,
  city,
  COUNT(DISTINCT order_id) AS orders,
  SUM(IF(status='COMPLETED', quantity, 0)) AS units_sold,
  ROUND(SUM(IF(status='COMPLETED', net_amount, 0)),2) AS net_revenue,
  ROUND(AVG(IF(status='COMPLETED', net_amount, NULL)),2) AS avg_order_value,
  COUNTIF(status='CANCELLED') AS cancelled_orders,
  COUNTIF(status='REFUNDED') AS refunded_orders,
  SAFE_DIVIDE(
    COUNTIF(status='CANCELLED'),
    COUNT(DISTINCT order_id)
  ) AS cancellation_rate
FROM `YOUR_GCP_PROJECT_ID.retail_dw.fct_order_line`
GROUP BY business_date, channel, city;
