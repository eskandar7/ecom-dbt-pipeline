CREATE OR REPLACE TABLE `titanium-diode-464605-j3.ecom_silver.reviews`
PARTITION BY DATE(review_time)
AS
SELECT
  CAST(review_id AS STRING) AS review_id,
  CAST(order_id AS STRING) AS order_id,
  CAST(product_id AS STRING) AS product_id,
  CAST(rating AS INT64) AS rating,
  review_text,
  TIMESTAMP(review_time) AS review_time
FROM `titanium-diode-464605-j3.ecom_bronze.reviews_ext`;
