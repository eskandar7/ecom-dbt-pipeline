CREATE OR REPLACE TABLE `titanium-diode-464605-j3.ecom_silver.events`
PARTITION BY DATE(timestamp)
AS
SELECT
  CAST(event_id AS STRING) AS event_id,
  CAST(session_id AS STRING) AS session_id,
  TIMESTAMP(timestamp) AS timestamp,
  event_type,
  CAST(product_id AS STRING) AS product_id,
  CAST(qty AS INT64) AS qty,
  CAST(cart_size AS INT64) AS cart_size,
  payment,
  CAST(discount_pct AS NUMERIC) AS discount_pct,
  CAST(amount_usd AS NUMERIC) AS amount_usd
FROM `titanium-diode-464605-j3.ecom_bronze.events_ext`;
