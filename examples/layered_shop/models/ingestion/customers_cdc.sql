{{ config(materialized='incremental', flow='auto_cdc', keys=['customer_id'],
          sequence_by='updated_at', stored_as='scd_type_1',
          apply_as_deletes="op = 'DELETE'") }}

select
    customer_id,
    name,
    email,
    updated_at,
    op
from {{ source('raw', 'customers_feed') }}
