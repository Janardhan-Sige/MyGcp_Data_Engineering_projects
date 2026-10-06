-- Streaming analytics example using event-time windows.
-- The Dataflow layer should apply the production windowing policy;
-- this SQL provides the downstream aggregation pattern for events already landed.

CREATE OR REPLACE TABLE `YOUR_GCP_PROJECT_ID.retail_mart.hourly_event_kpis`
PARTITION BY event_date
CLUSTER BY city, event_type
AS
SELECT
  DATE(event_ts) AS event_date,
  TIMESTAMP_TRUNC(event_ts, HOUR) AS event_hour,
  city,
  event_type,
  COUNT(*) AS event_count,
  COUNT(DISTINCT order_id) AS distinct_orders,
  COUNT(DISTINCT customer_id) AS distinct_customers
FROM `YOUR_GCP_PROJECT_ID.retail_raw.raw_order_events`
WHERE event_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
GROUP BY event_date, event_hour, city, event_type;
