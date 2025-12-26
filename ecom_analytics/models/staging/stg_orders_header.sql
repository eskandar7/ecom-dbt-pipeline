{{ config(materialized='view') }}

select
  cast(event_ts as timestamp) as event_ts,
  cast(event_type as string)  as event_type,
  cast(dt as date)            as dt,
  cast(hr as int64)           as hr,

  cast(order_id as string)        as order_id,
  cast(customer_id as string)     as customer_id,
  cast(country as string)         as country,
  cast(currency as string)        as currency,
  cast(payment_method as string)  as payment_method,
  cast(amount as numeric)         as order_amount

from {{ source('ecom_silver_prod', 'orders_header_ext') }}
