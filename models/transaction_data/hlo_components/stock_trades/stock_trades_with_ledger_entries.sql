{{  config(
        materialized='incremental',
        unique_key='ledger_entry_id',
        on_schema_change='append_new_columns', 
        schema='intermediate') }}

select
    stock_trades_with_all_transfer_ids.hlo_id,
    stock_trades_with_all_transfer_ids.hlo_table,
    stock_trades_with_all_transfer_ids.hlo_journal_id,
    stock_trades_with_all_transfer_ids.hlo_status,
    stock_trades_with_all_transfer_ids.hlo_created_at,
    stock_trades_with_all_transfer_ids.hlo_updated_at,
    stock_trades_with_all_transfer_ids.transfer_id,
    stock_trades_with_all_transfer_ids.is_original_transfer_reversed,
    stock_trades_with_all_transfer_ids.is_transfer_reversal,
    ledger_entries.id as ledger_entry_id,
    ledger_entries.journal_id as ledger_entry_journal_id,
    ledger_entries.amount,
    ledger_entries.currency,
    ledger_entries.user_id,
    ledger_entries.timestamp,
    case
        when
            is_transfer_reversal = true then concat(hlo_table, '_', 'REVERSAL')
        else concat(hlo_table, '_', stock_trades.position, '_', hlo_status)
    end as transfer_type,
    case when ledger_entries.amount < 0 then true else false end as is_debit,
    case when ledger_entries.amount < 0 then ledger_entries.user_id end as outgoing_user_id,
    case when ledger_entries.amount > 0 then ledger_entries.user_id end as incoming_user_id
    
from
    {{ ref('stock_trades_with_all_transfer_ids') }} as stock_trades_with_all_transfer_ids
inner join
    {{ ref('ledger_entries') }} as ledger_entries on
        ledger_entries.journal_id = stock_trades_with_all_transfer_ids.hlo_journal_id

inner join
    "CHIPPER".{{ var("core_public") }}."STOCK_TRADES" on
        ledger_entries.journal_id = stock_trades.journal_id

where
    ledger_entries.user_id not like 'base-%'
    and (
        (
            ledger_entries.amount < 0
            and is_transfer_reversal = false
            and stock_trades.position in ('BUY', 'DIVTAX', 'REWARD', 'CANCELLED', 'MERGER_EXCHANGE_STOCK_CASH')
        )
        or (
            ledger_entries.amount > 0
            and is_transfer_reversal = true
            and stock_trades.position in ('BUY', 'DIVTAX', 'REWARD', 'CANCELLED', 'MERGER_EXCHANGE_STOCK_CASH')
        )
        or (
            ledger_entries.amount > 0
            and is_transfer_reversal = false
            and stock_trades.position in ('SELL', 'DIV', 'REWARD', 'CANCELLED', 'MERGER_EXCHANGE_STOCK_CASH')

        )
        or (
            ledger_entries.amount < 0
            and is_transfer_reversal = true
            and stock_trades.position in ('SELL', 'DIV', 'REWARD', 'CANCELLED', 'MERGER_EXCHANGE_STOCK_CASH')
        )
    )

    {% if is_incremental() %}
        -- this filter will only be applied on an incremental run
        and hlo_updated_at >= (select max(hlo_updated_at) from {{ this }})
    {% endif %}
