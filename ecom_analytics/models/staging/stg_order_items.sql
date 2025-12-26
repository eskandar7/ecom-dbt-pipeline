{{ config(materialized='view') }}

select
  cast(dt as date)   as dt,
  cast(hr as int64)  as hr,

  cast(order_id as string)     as order_id,
  cast(sku as string)          as sku,
  cast(qty as int64)           as qty,
  cast(unit_price as numeric)  as unit_price,

  cast(qty as numeric) * cast(unit_price as numeric) as line_amount

from {{ source('ecom_silver_prod', 'order_items_ext') }}
