CREATE OR REPLACE TABLE `titanium-diode-464605-j3.ecom_silver.sessions`
PARTITION BY DATE(start_time)
AS
SELECT
  CAST(session_id AS STRING) AS session_id,
  CAST(customer_id AS STRING) AS customer_id,
  TIMESTAMP(start_time) AS start_time,
  device,
  source,
  country
FROM `titanium-diode-464605-j3.ecom_bronze.sessions_ext`;
