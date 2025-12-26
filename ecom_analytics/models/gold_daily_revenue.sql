{{ config(materialized='table') }}

select
  dt,
  count(distinct order.order_id) as orders,
  sum(order.amount) as revenue
from {{ source('ecom_silver_prod', 'orders_ext') }}
group by dt
order by dt
