CREATE OR REPLACE TABLE `titanium-diode-464605-j3.ecom_silver.customers`
PARTITION BY signup_date
AS
SELECT
  CAST(customer_id AS STRING) AS customer_id,
  name,
  email,
  country,
  CAST(age AS INT64) AS age,
  DATE(signup_date) AS signup_date,
  CAST(marketing_opt_in AS BOOL) AS marketing_opt_in
FROM `titanium-diode-464605-j3.ecom_bronze.customers_ext`;
