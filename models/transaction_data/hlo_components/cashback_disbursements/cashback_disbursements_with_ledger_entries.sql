{{ config(
        materialized='incremental',
        unique_key='ledger_entry_id',
        on_schema_change='append_new_columns', 
        schema='intermediate'
    )
}}

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
    ledger_entries.id as ledger_entry_id,
    ledger_entries.journal_id as ledger_entry_journal_id,
    ledger_entries.currency,
    ledger_entries.amount,
    ledger_entries.user_id,
    ledger_entries.timestamp,
    case when is_transfer_reversal = true then 'CASHBACKS_REVERSAL'
        else concat('CASHBACKS_', hlo_status)
    end as transfer_type,
    case when amount < 0 then true
        else false
    end as is_debit,
    case when amount < 0 then user_id end as outgoing_user_id,
    case when amount > 0 then user_id end as incoming_user_id
from {{ ref('cashback_disbursements_with_all_transfer_ids') }} as cashback_disbursements_hlo
inner join {{ ref('ledger_entries') }} as ledger_entries
    on cashback_disbursements_hlo.hlo_journal_id = ledger_entries.journal_id
where
    ledger_entries.user_id not like 'base-%'
    and ledger_entries.user_id not like 'issuer-%'
    and amount != 0
    {% if is_incremental() %}
        -- this filter will only be applied on an incremental run
        and hlo_updated_at >= (select max(hlo_updated_at) from {{ this }})
    {% endif %}
