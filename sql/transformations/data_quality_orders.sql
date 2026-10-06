-- Data-quality gate.
-- Bad records are isolated instead of silently disappearing.
-- The result can be consumed by an Airflow quality-check task.

CREATE OR REPLACE TABLE `YOUR_GCP_PROJECT_ID.retail_ops.rejected_order_lines`
PARTITION BY DATE(ingestion_ts)
AS
SELECT
  *,
  ARRAY_TO_STRING(
    ARRAY(
      SELECT rule
      FROM UNNEST([
        IF(order_id IS NULL OR order_id = '', 'MISSING_ORDER_ID', NULL),
        IF(order_line_id IS NULL OR order_line_id = '', 'MISSING_ORDER_LINE_ID', NULL),
        IF(quantity IS NULL OR quantity <= 0, 'INVALID_QUANTITY', NULL),
        IF(unit_price IS NULL OR unit_price < 0, 'INVALID_UNIT_PRICE', NULL),
        IF(status NOT IN ('COMPLETED','CANCELLED','REFUNDED'), 'INVALID_STATUS', NULL)
      ]) rule
      WHERE rule IS NOT NULL
    ), ','
  ) AS dq_failure_reason,
  CURRENT_TIMESTAMP() AS dq_checked_ts
FROM `YOUR_GCP_PROJECT_ID.retail_raw.raw_order_lines`
WHERE
  order_id IS NULL OR order_id = ''
  OR order_line_id IS NULL OR order_line_id = ''
  OR quantity IS NULL OR quantity <= 0
  OR unit_price IS NULL OR unit_price < 0
  OR status NOT IN ('COMPLETED','CANCELLED','REFUNDED');

CREATE OR REPLACE TABLE `YOUR_GCP_PROJECT_ID.retail_ops.dq_run_summary` AS
SELECT
  CURRENT_TIMESTAMP() AS check_ts,
  COUNT(*) AS rejected_rows,
  COUNTIF(dq_failure_reason LIKE '%MISSING_ORDER_ID%') AS missing_order_id,
  COUNTIF(dq_failure_reason LIKE '%INVALID_QUANTITY%') AS invalid_quantity,
  COUNTIF(dq_failure_reason LIKE '%INVALID_UNIT_PRICE%') AS invalid_price,
  COUNTIF(dq_failure_reason LIKE '%INVALID_STATUS%') AS invalid_status
FROM `YOUR_GCP_PROJECT_ID.retail_ops.rejected_order_lines`;
