{{ config(materialized='table') }}

-- cross-pipeline read: orders_st and customers_cdc are owned by `bronze`.
-- Inside the `gold` pipeline they resolve to plain table reads.
select
    c.customer_id,
    c.name,
    count(o.order_id) as order_count,
    sum(o.amount) as lifetime_value
from {{ ref('orders_st') }} as o
join {{ ref('customers_cdc') }} as c
    on o.customer_id = c.customer_id
group by c.customer_id, c.name
