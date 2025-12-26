{{ config(
    materialized='table',
    partition_by={"field": "dt", "data_type": "date"},
    cluster_by=["order_id"]
) }}

select
  dt,
  hr,
  event_ts,
  event_type,
  order_id,
  customer_id,
  country,
  currency,
  payment_method,
  order_amount
from {{ ref('stg_orders_header') }}
