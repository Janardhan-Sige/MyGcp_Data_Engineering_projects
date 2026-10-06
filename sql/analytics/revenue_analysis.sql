-- ============================================================
-- Product revenue analysis.
-- Demonstrates window functions and percentage contribution.
-- ============================================================

WITH product_revenue AS (
  SELECT
    product_id,
    ROUND(SUM(order_amount), 2) AS revenue
  FROM `YOUR_GCP_PROJECT_ID.retail_dw.clean_orders`
  WHERE status = "COMPLETED"
  GROUP BY product_id
)
SELECT
  product_id,
  revenue,
  ROUND(
    100 * SAFE_DIVIDE(revenue, SUM(revenue) OVER ()),
    2
  ) AS revenue_percentage
FROM product_revenue
ORDER BY revenue DESC;
