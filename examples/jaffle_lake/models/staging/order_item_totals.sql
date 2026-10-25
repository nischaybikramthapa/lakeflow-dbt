{{ config(materialized='ephemeral') }}

-- inlined as a CTE wherever it is ref()'d; never becomes a dataset
select
    order_id,
    sum(amount) as order_total
from {{ source('raw', 'payments') }}
group by order_id
