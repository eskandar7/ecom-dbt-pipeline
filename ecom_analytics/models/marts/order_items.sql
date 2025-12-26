{{ config(
    materialized='table',
    partition_by={"field": "dt", "data_type": "date"},
    cluster_by=["order_id", "sku"]
) }}

select
  dt,
  hr,
  order_id,
  sku,
  qty,
  unit_price,
  line_amount
from {{ ref('stg_order_items') }}
