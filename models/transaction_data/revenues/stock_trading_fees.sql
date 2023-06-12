{{ config(materialized='ephemeral') }}

with stock_trading_fees_without_transfer_quotes as (
    select
        stock_trades.transfer_id,
        stock_trades.journal_id,
        null as fee_calculation_id,
        null as fee_config_id,
        null as forex_fee_calculation_id,
        null as transfer_quote_id,
        'DRIVEWEALTH' as external_provider,
        order_response:id::varchar as external_provider_transaction_id,

        hlo_created_at as transaction_created_at,
        hlo_updated_at as transaction_updated_at,

        'STOCK_TRADING_FEES' as revenue_stream,
        fee_currency as revenue_currency,
        null as exchange_rate_fee_percentage_in_decimals,
        null as commission_revenue_rate_in_decimals,
        fee_amount as gross_revenues,
        null as sales_discount_percentage_in_decimals,
        null as sales_discount,
        gross_revenues as net_revenues,

        case when revenue_currency = expanded_transfers.destination_currency then expanded_transfers.destination_rate
            when revenue_currency = expanded_transfers.origin_currency then expanded_transfers.origin_rate
            else null
        end as rate_to_usd,
        gross_revenues * rate_to_usd as gross_revenues_in_usd,
        sales_discount * rate_to_usd as sales_discount_in_usd,
        net_revenues * rate_to_usd as net_revenues_in_usd,

        user_id as monetized_user_id
    from "CHIPPER".{{ var("core_public") }}."STOCK_TRADES" as stock_trades
    join "CHIPPER"."DBT_TRANSFORMATIONS"."EXPANDED_TRANSFERS" as expanded_transfers
        on stock_trades.transfer_id = expanded_transfers.transfer_id
    left join "CHIPPER".{{ var("core_public") }}."TRANSFER_QUOTES" as transfer_quotes
        on expanded_transfers.transfer_id = transfer_quotes.transfer_id
    where 
        stock_trades.status = 'SETTLED'
        and position in ('BUY', 'SELL')
        and reverse_transfer_id is null
        and transfer_quotes.transfer_id is null
),

stock_trading_fees_using_transfer_quotes as (
    select
        expanded_transfers.transfer_id,
        expanded_transfers.journal_id,
        fee_calculations.id as fee_calculation_id,
        fee_calculations.fee_config_id,
        null as forex_fee_calculation_id,
        transfer_quotes.id as transfer_quote_id,
        'DRIVEWEALTH' as external_provider,
        order_response:id::varchar as external_provider_transaction_id,

        hlo_created_at as transaction_created_at,
        hlo_updated_at as transaction_updated_at,

        'STOCK_TRADING_FEES' as revenue_stream,
        fee_calculations.currency as revenue_currency,
        null as exchange_rate_fee_percentage_in_decimals,
        null as commission_revenue_rate_in_decimals,
        fee_calculations.debited_amount as gross_revenues,
        null as sales_discount_percentage_in_decimals,
        null as sales_discount,
        gross_revenues as net_revenues,

        case when revenue_currency = expanded_transfers.destination_currency then destination_rate
            when revenue_currency = expanded_transfers.origin_currency then origin_rate
            else null
        end as rate_to_usd,
        gross_revenues * rate_to_usd as gross_revenues_in_usd,
        sales_discount * rate_to_usd as sales_discount_in_usd,
        net_revenues * rate_to_usd as net_revenues_in_usd,

        user_id as monetized_user_id
    from "CHIPPER".{{ var("core_public") }}."FEE_CALCULATIONS" as fee_calculations
    inner join "CHIPPER".{{ var("core_public") }}."TRANSFER_QUOTES" as transfer_quotes
        on fee_calculations.transfer_quote_id = transfer_quotes.id
    inner join "CHIPPER"."DBT_TRANSFORMATIONS"."EXPANDED_TRANSFERS" as expanded_transfers
        on transfer_quotes.transfer_id = expanded_transfers.transfer_id
    inner join "CHIPPER".{{ var("core_public") }}."STOCK_TRADES" as stock_trades
        on expanded_transfers.transfer_id = stock_trades.transfer_id
    where
        is_original_transfer_reversed = false
        and transfer_type in ('STOCK_TRADES_BUY_SETTLED', 'STOCK_TRADES_SELL_SETTLED')
        and origin_amount_in_usd != destination_amount_in_usd
        and debited_amount != 0
)

select *
from stock_trading_fees_without_transfer_quotes

union

select *
from stock_trading_fees_using_transfer_quotes
