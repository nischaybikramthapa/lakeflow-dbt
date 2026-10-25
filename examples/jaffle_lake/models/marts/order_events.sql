{{ config(materialized='incremental') }}

select
    o.order_id,
    o.customer_id,
    o.order_date,
    o.status,
    o.amount,
    t.order_total
from {{ ref('stg_orders') }} as o
left join {{ ref('order_item_totals') }} as t
    on o.order_id = t.order_id
