{{ config(materialized='table', tblproperties={'quality': 'gold'}) }}

select
    order_date,
    count(distinct order_id) as orders,
    sum(amount) as revenue
from {{ ref('order_events') }}
group by order_date
