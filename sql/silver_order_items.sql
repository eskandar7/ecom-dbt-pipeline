CREATE OR REPLACE TABLE `titanium-diode-464605-j3.ecom_silver.order_items`
AS
SELECT
  CAST(order_id AS STRING) AS order_id,
  CAST(product_id AS STRING) AS product_id,
  CAST(unit_price_usd AS NUMERIC) AS unit_price_usd,
  CAST(quantity AS INT64) AS quantity,
  CAST(line_total_usd AS NUMERIC) AS line_total_usd
FROM `titanium-diode-464605-j3.ecom_bronze.order_items_ext`;
