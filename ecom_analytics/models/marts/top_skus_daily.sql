{{ config(
    materialized='table',
    partition_by={"field": "dt", "data_type": "date"},
    cluster_by=["sku"]
) }}

with sku_day as (
  select
    dt,
    sku,
    sum(line_amount) as sku_revenue,
    sum(qty)         as sku_qty,
    count(*)         as lines
  from {{ ref('order_items') }}
  group by dt, sku
),

ranked as (
  select
    *,
    row_number() over(partition by dt order by sku_revenue desc) as sku_rank
  from sku_day
)

select
  dt,
  sku,
  sku_revenue,
  sku_qty,
  lines,
  sku_rank
from ranked
where sku_rank <= 50
