{{ config(materialized='table') }}

with orders as (
  select * from {{ ref('stg_orders_header') }}
),

items as (
  select
    order_id,
    sum(line_amount) as items_amount,
    sum(qty)         as items_qty,
    count(*)         as lines
  from {{ ref('stg_order_items') }}
  group by order_id
)

select
  o.dt,
  o.hr,
  o.event_ts,
  o.event_type,

  o.order_id,
  o.customer_id,
  o.country,
  o.currency,
  o.payment_method,

  o.order_amount,
  i.items_amount,
  i.items_qty,
  i.lines

from orders o
left join items i using(order_id)

