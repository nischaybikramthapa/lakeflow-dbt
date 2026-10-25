{{ config(
    materialized='snapshot',
    keys=['sku'],
    version='snapshot_ts',
    stored_as='scd_type_2',
) }}

select
    sku,
    warehouse,
    on_hand,
    snapshot_ts
from {{ source('raw', 'inventory') }}
