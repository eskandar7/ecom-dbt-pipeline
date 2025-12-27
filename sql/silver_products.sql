CREATE OR REPLACE TABLE `titanium-diode-464605-j3.ecom_silver.products`
AS
SELECT
  CAST(product_id AS STRING) AS product_id,
  category,
  name,
  CAST(price_usd AS NUMERIC) AS price_usd,
  CAST(cost_usd AS NUMERIC) AS cost_usd,
  CAST(margin_usd AS NUMERIC) AS margin_usd
FROM `titanium-diode-464605-j3.ecom_bronze.products_ext`;
