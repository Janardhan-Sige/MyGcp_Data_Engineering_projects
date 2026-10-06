-- Pipeline audit table.
-- Every production-style orchestration should record what ran, when, how much,
-- and whether the business-quality gate passed.

CREATE TABLE IF NOT EXISTS `YOUR_GCP_PROJECT_ID.retail_ops.pipeline_run_audit` (
  run_id STRING NOT NULL,
  pipeline_name STRING NOT NULL,
  pipeline_type STRING NOT NULL,
  execution_date DATE,
  started_at TIMESTAMP NOT NULL,
  completed_at TIMESTAMP,
  status STRING NOT NULL,
  source_rows INT64,
  accepted_rows INT64,
  rejected_rows INT64,
  duplicate_rows INT64,
  target_table STRING,
  dataflow_job_id STRING,
  error_message STRING
)
PARTITION BY execution_date
CLUSTER BY pipeline_name, status;

-- Example post-run validation query.
SELECT
  pipeline_name,
  status,
  source_rows,
  accepted_rows,
  rejected_rows,
  duplicate_rows,
  SAFE_DIVIDE(rejected_rows, source_rows) AS rejection_rate
FROM `YOUR_GCP_PROJECT_ID.retail_ops.pipeline_run_audit`
WHERE execution_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
ORDER BY started_at DESC;
