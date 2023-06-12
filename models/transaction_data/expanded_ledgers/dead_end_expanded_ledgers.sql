{{ config(
        materialized='table',
        schema='intermediate') }}

select
    ledger_entries.id as ledger_entry_id,
    null::int as transfer_id,
    null as is_original_transfer_reversed,
    null as is_transfer_reversal,
    'DEAD_END_LEDGER_ENTRY' as transfer_type,
    ledger_entries.journal_id as journal_id,
    journals.type as journal_type,
    null as hlo_id,
    null as hlo_table,
    null as hlo_status,
    null as corridor,
    null as hlo_created_at,
    null as hlo_updated_at,
    ledger_entries.currency as ledger_currency,
    ledger_entries.amount as ledger_amount,
    null as ledger_rate,
    null as ledger_amount_in_usd,
    ledger_entries.timestamp as ledger_timestamp,
    ledger_entries.user_id as main_party_user_id,
    null as counter_party_user_id,
    'DEAD_END_LEDGER_ENTRY' as activity_type

from {{ ref('ledger_entries') }} as ledger_entries
left join {{ ref('hlo_expanded_ledgers') }} as hlo_expanded_ledgers on 
    hlo_expanded_ledgers.ledger_entry_id = ledger_entries.id
left join "CHIPPER".{{ var("core_public") }}."JOURNALS" as journals
    on ledger_entries.journal_id = journals.id
where
    user_id not like 'base-%'
    and user_id not like 'issuer-%'
    and amount != 0
    and hlo_expanded_ledgers.ledger_entry_id is null
