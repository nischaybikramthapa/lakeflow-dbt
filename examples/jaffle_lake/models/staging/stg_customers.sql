{{ config(materialized='view') }}

select
    customer_id,
    lower(trim(name)) as name,
    lower(trim(email)) as email
from {{ source('raw', 'customers') }}
