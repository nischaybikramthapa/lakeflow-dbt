{{ config(materialized='incremental') }}

select
    order_id,
    customer_id,
    order_date,
    status,
    amount
from {{ source('raw', 'orders') }}
