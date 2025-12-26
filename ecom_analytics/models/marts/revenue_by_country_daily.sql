{{ config(
    materialized='table',
    partition_by={"field": "dt", "data_type": "date"},
    cluster_by=["country"]
) }}

select
  dt,
  country,
  count(distinct order_id) as orders,
  sum(order_amount)        as revenue
from {{ ref('orders_header') }}
group by dt, country


