{{ config(
    materialized='table',
    partition_by={"field": "dt", "data_type": "date"}
) }}

select
  dt,
  count(distinct order_id) as orders,
  sum(order_amount)        as revenue,
  sum(coalesce(items_qty, 0)) as items_qty,
  sum(coalesce(lines, 0))     as lines
from {{ ref('fct_orders') }}
group by dt
