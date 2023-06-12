{{ config(materialized='ephemeral') }}

select
    payments_hlo.hlo_id,
    payments_hlo.hlo_table,
    payments_hlo.hlo_journal_id,
    payments_hlo.hlo_status,
    payments_hlo.hlo_created_at,
    payments_hlo.hlo_updated_at,
    payments_hlo.transfer_id,
    payments_hlo.is_original_transfer_reversed,
    payments_hlo.is_transfer_reversal,
    ledger_entries.id as ledger_entry_id,
    ledger_entries.journal_id as ledger_entry_journal_id,
    ledger_entries.amount,
    ledger_entries.currency,
    ledger_entries.user_id,
    ledger_entries.timestamp,
    payment_type as transfer_type,
    case
        when amount < 0 then true 
        else false
    end as is_debit,
    case when amount < 0 then user_id end as outgoing_user_id,
    case when amount > 0 then user_id end as incoming_user_id,
    row_number() over (
        partition by ledger_entry_journal_id, transfer_id, is_debit order by ledger_entry_id asc
    ) as row_num_asc,
    row_number() over (
        partition by ledger_entry_journal_id, transfer_id, is_debit order by ledger_entry_id desc
    ) as row_num_desc

from {{ ref('payments_with_type_with_all_transfer_ids') }} as payments_hlo

join {{ ref('ledger_entries') }} as ledger_entries
    on ledger_entries.journal_id = payments_hlo.hlo_journal_id

where 
    ledger_entries.user_id not like 'base-%'
    and amount != 0
