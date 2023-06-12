{{  config(
        materialized='incremental',
        unique_key='transfer_id',
        on_schema_change='append_new_columns') }}

select
    transfers_with_usd.id as transfer_id,
    is_original_transfer_reversed,
    is_transfer_reversal,
    transfer_type,
    transfers_with_usd.status as transfer_status,
    transfers_with_usd.created_at as transfer_created_at,
    transfers_with_usd.updated_at as transfer_updated_at,
    expanded_ledgers_incremental.journal_id,
    journal_type,
    hlo_id,
    hlo_table,
    hlo_status,
    hlo_created_at,
    hlo_updated_at,
    origin_currency,
    origin_amount,
    origin_rate_id,
    origin_rate,
    origin_amount_in_usd,
    max(case when ledger_amount < 0 then main_party_user_id else null end) as outgoing_user_id,
    exchange_rate_fee_percentage,
    base_modification_percentage,
    exchange_rate,
    transfers_with_usd.corridor,
    destination_currency,
    destination_amount,
    destination_rate_id,
    destination_rate,
    destination_amount_in_usd,
    max(case when ledger_amount > 0 then main_party_user_id else null end) as incoming_user_id,
    flat_fee_currency,
    flat_fee_amount,
    flat_fee_rate,
    flat_fee_amount_in_usd
    from {{ref('expanded_ledgers')}} as expanded_ledgers_incremental
join {{ref('transfers_with_usd')}} as transfers_with_usd
 on transfers_with_usd.id = expanded_ledgers_incremental.transfer_id
where transfer_type <> 'DEAD_END_LEDGER_ENTRY'
{% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    and hlo_updated_at >= (select max(hlo_updated_at) from {{ this }})
{% endif %}
group by
    transfers_with_usd.id,
    is_original_transfer_reversed,
    is_transfer_reversal,
    transfer_type,
    transfer_status,
    transfer_created_at,
    transfer_updated_at,
    expanded_ledgers_incremental.journal_id,
    journal_type,
    hlo_id,
    hlo_table,
    hlo_status,
    hlo_created_at,
    hlo_updated_at,
    origin_currency,
    origin_amount,
    origin_rate_id,
    origin_rate,
    origin_amount_in_usd,
    exchange_rate_fee_percentage,
    base_modification_percentage,
    exchange_rate,
    transfers_with_usd.corridor,
    destination_currency,
    destination_amount,
    destination_rate_id,
    destination_rate,
    destination_amount_in_usd,
    flat_fee_currency,
    flat_fee_amount,
    flat_fee_rate,
    flat_fee_amount_in_usd
    
