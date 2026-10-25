{{ config(materialized='table', cluster_by=['customer_id']) }}

select
    c.customer_id,
    c.name,
    c.email,
    count(o.order_id) as order_count,
    coalesce(sum(o.amount), 0) as lifetime_value,
    '{{ var("currency") }}' as currency
from {{ ref('stg_customers') }} as c
left join {{ ref('stg_orders') }} as o
    on c.customer_id = o.customer_id
group by c.customer_id, c.name, c.email
