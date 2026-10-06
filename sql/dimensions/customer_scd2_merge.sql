-- SCD Type 2 customer dimension.
-- A new version is created when tracked attributes change.
-- The previous version is closed without overwriting history.

MERGE `YOUR_GCP_PROJECT_ID.retail_dw.dim_customer` T
USING (
  SELECT
    FARM_FINGERPRINT(customer_id) AS customer_sk,
    customer_id,
    customer_name,
    email,
    segment,
    city,
    state,
    TIMESTAMP(updated_at) AS effective_from,
    TO_HEX(SHA256(CONCAT(
      COALESCE(customer_name,''),'|',
      COALESCE(email,''),'|',
      COALESCE(segment,''),'|',
      COALESCE(city,''),'|',
      COALESCE(state,'')
    ))) AS record_hash,
    source_system,
    CURRENT_TIMESTAMP() AS ingestion_ts
  FROM `YOUR_GCP_PROJECT_ID.retail_raw.raw_customers`
) S
ON T.customer_id = S.customer_id AND T.is_current = TRUE

WHEN MATCHED AND T.record_hash != S.record_hash THEN
  UPDATE SET
    effective_to = S.effective_from,
    is_current = FALSE

WHEN NOT MATCHED THEN
  INSERT (
    customer_sk, customer_id, customer_name, email, segment, city, state,
    effective_from, effective_to, is_current, record_hash, source_system, ingestion_ts
  )
  VALUES (
    S.customer_sk, S.customer_id, S.customer_name, S.email, S.segment, S.city, S.state,
    S.effective_from, NULL, TRUE, S.record_hash, S.source_system, S.ingestion_ts
  );
