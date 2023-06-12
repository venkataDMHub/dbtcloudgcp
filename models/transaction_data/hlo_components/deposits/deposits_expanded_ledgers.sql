{{  config(
        materialized='incremental',
        unique_key='ledger_entry_id',
        on_schema_change='append_new_columns', 
        schema='intermediate') }}

select
    ledger_entry_id,
    transfer_id,
    is_original_transfer_reversed,
    is_transfer_reversal,
    transfer_type,
    hlo_journal_id as journal_id,
    journals.type as journal_type,
    hlo_id,
    hlo_table,
    hlo_status,
    corridor,

    hlo_created_at,
    hlo_updated_at,
    currency as ledger_currency,
    amount as ledger_amount,

    timestamp as ledger_timestamp,

    user_id as main_party_user_id,

    null as counter_party_user_id,
    case when ledger_amount < 0 then origin_rate
        else destination_rate end as ledger_rate,
    amount * ledger_rate as ledger_amount_in_usd

from
    {{ ref('deposits_with_ledger_entries') }} as deposits_with_ledger_entries
inner join
    "CHIPPER".{{ var("core_public") }}."JOURNALS"
    on journals.id = deposits_with_ledger_entries.hlo_journal_id

inner join
    {{ ref('transfers_with_usd') }} as transfers_with_usd
    on deposits_with_ledger_entries.transfer_id = transfers_with_usd.id

{% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where hlo_updated_at >= (select max(hlo_updated_at) from {{ this }})
{% endif %}
