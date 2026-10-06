-- ============================================================
-- Create the BigQuery dataset used by this learning project.
-- Run this in the BigQuery SQL editor.
-- Replace YOUR_GCP_PROJECT_ID with your real project ID.
-- ============================================================

CREATE SCHEMA IF NOT EXISTS `YOUR_GCP_PROJECT_ID.retail_dw`
OPTIONS(
  location = "US",
  description = "Retail data warehouse for the GCP Data Engineering project"
);
