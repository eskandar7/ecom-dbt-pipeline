CREATE OR REPLACE TABLE `titanium-diode-464605-j3.ecom_silver.orders`
PARTITION BY DATE(order_time)
AS
SELECT
  CAST(order_id AS STRING) AS order_id,
  CAST(customer_id AS STRING) AS customer_id,
  TIMESTAMP(order_time) AS order_time,
  payment_method,
  CAST(discount_pct AS INT64) AS discount_pct,
  CAST(subtotal_usd AS NUMERIC) AS subtotal_usd,
  CAST(total_usd AS NUMERIC) AS total_usd,
  country,
  device,
  source
FROM `titanium-diode-464605-j3.ecom_bronze.orders_ext`;
