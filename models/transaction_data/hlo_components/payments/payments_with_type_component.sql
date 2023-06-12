{{ config(materialized='ephemeral') }}

select
    hlo_id,
    hlo_table,
    hlo_journal_id,
    hlo_status,
    hlo_created_at,
    hlo_updated_at,
    transfer_id,
    is_original_transfer_reversed,
    is_transfer_reversal,
    transfer_type,
    ledger_entry_journal_id,
    max(case when row_num_desc = 1 then incoming_user_id end) as incoming_user_id,
    min(case when row_num_asc = 1 then outgoing_user_id end) as outgoing_user_id

from {{ ref('payments_with_type_with_ledger_entries') }}
group by
    hlo_id,
    hlo_table,
    hlo_journal_id,
    hlo_status,
    hlo_created_at,
    hlo_updated_at,
    transfer_id,
    is_original_transfer_reversed,
    is_transfer_reversal,
    transfer_type,
    ledger_entry_journal_id
