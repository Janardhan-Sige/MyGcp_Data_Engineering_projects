-- ============================================================
-- Customer-level analytics.
-- Useful interview topics: CTEs, window functions, ranking,
-- aggregation, and customer segmentation.
-- ============================================================

WITH customer_sales AS (
  SELECT
    customer_id,
    COUNT(*) AS order_count,
    ROUND(SUM(order_amount), 2) AS total_value
  FROM `YOUR_GCP_PROJECT_ID.retail_dw.clean_orders`
  WHERE status = "COMPLETED"
  GROUP BY customer_id
)
SELECT
  customer_id,
  order_count,
  total_value,
  DENSE_RANK() OVER (ORDER BY total_value DESC) AS customer_value_rank,
  CASE
    WHEN total_value >= 500 THEN "HIGH_VALUE"
    WHEN total_value >= 200 THEN "MEDIUM_VALUE"
    ELSE "STANDARD"
  END AS customer_segment
FROM customer_sales
ORDER BY customer_value_rank;
