-- ============================================================
-- Daily sales KPI query.
-- Demonstrates GROUP BY, conditional aggregation, and KPIs.
-- ============================================================

SELECT
  DATE(order_ts) AS order_date,
  COUNT(*) AS total_orders,
  COUNTIF(status = "COMPLETED") AS completed_orders,
  COUNTIF(status = "CANCELLED") AS cancelled_orders,
  ROUND(SUM(IF(status = "COMPLETED", order_amount, 0)), 2) AS completed_revenue,
  ROUND(AVG(IF(status = "COMPLETED", order_amount, NULL)), 2) AS avg_completed_order_value
FROM `YOUR_GCP_PROJECT_ID.retail_dw.clean_orders`
GROUP BY order_date
ORDER BY order_date;
