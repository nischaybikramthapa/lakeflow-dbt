-- A second flow appending into the `order_events` streaming table.
-- SDP supports many flows per target; `into:` makes this model flow-only
-- (no CREATE TABLE of its own).
{{ config(materialized='incremental', into='order_events') }}

select
    order_id,
    customer_id,
    order_date,
    status,
    amount,
    cast(null as double) as order_total   -- EU feed has no item totals
from {{ source('raw', 'orders_eu') }}
