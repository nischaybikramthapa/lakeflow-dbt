{{ config(
    materialized='incremental',
    flow='auto_cdc',
    keys=['customer_id'],
    sequence_by='updated_at',
    stored_as='scd_type_2',
    track_history={'except': ['updated_at']},
    apply_as_deletes="op = 'DELETE'",
    ignore_null_updates=true,
) }}

select
    customer_id,
    name,
    email,
    updated_at,
    op
from {{ source('raw', 'customers_feed') }}
