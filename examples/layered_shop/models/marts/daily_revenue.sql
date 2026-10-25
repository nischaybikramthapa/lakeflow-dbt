{{ config(materialized='table') }}

select
    o.order_date,
    s.label as status_label,
    sum(o.amount) as revenue
from {{ ref('orders_st') }} as o
join {{ ref('status_codes') }} as s
    on o.status = s.status
group by o.order_date, s.label
