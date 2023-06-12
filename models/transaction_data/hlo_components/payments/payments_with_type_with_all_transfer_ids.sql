{{ config(materialized='ephemeral') }}

select hlo_id::text as hlo_id,
    hlo_table,
    hlo_journal_id,
    hlo_status,
    hlo_created_at,
    hlo_updated_at,
    transfer_id,
    is_original_transfer_reversed,
    is_transfer_reversal,
    payment_type
from {{ ref('payments_with_type') }}
