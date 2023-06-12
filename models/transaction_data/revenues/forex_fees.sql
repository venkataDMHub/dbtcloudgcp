{{ config(materialized='ephemeral') }}

{% set accepted_transfer_types = (
        'AIRTIME_PURCHASES_COMPLETED',
        'BILL_PAYMENTS_COMPLETED',
        'CHECKOUTS_SETTLED',
        'DATA_PURCHASES_COMPLETED',
        'DEPOSITS_SETTLED',
        'ISSUED_CARD_TRANSACTIONS_FUNDING_COMPLETED',
        'ISSUED_CARD_TRANSACTIONS_WITHDRAWAL_COMPLETED',
        'NETWORK_API_B2C_SETTLED',
        'NETWORK_API_C2B_SETTLED',
        'PAYMENTS_P2P_SETTLED',
        'PAYMENT_INVITATIONS_SETTLED',
        'REQUESTS_SETTLED',
        'S2NC_SETTLED',
        'STOCK_TRADES_BUY_SETTLED',
        'STOCK_TRADES_DIVTAX_SETTLED',
        'STOCK_TRADES_DIV_SETTLED',
        'STOCK_TRADES_MERGER_EXCHANGE_STOCK_CASH_SETTLED',
        'STOCK_TRADES_SELL_SETTLED',
        'WITHDRAWALS_SETTLED'
    )
%}

with forex_fees_without_transfer_quotes as (
    select
        expanded_transfers.transfer_id,
        journal_id,
        null as fee_calculation_id,
        null as fee_config_id,
        null as forex_fee_calculation_id,
        null as transfer_quote_id,

        {# /* Null for now, but can be left-joined soon with the transaction_details transformation to populate provider info */ #}
        null as external_provider,
        null as external_provider_transaction_id,

        hlo_created_at as transaction_created_at,
        hlo_updated_at as transaction_updated_at,

        'FOREX_FEES' as revenue_stream,
        expanded_transfers.origin_currency as revenue_currency,
        (exchange_rate_fee_percentage / 100) as exchange_rate_fee_percentage_in_decimals,
        null as commission_revenue_rate_in_decimals,
        exchange_rate_fee_percentage_in_decimals * expanded_transfers.origin_amount as gross_revenues,
        null as sales_discount_percentage_in_decimals,
        null as sales_discount,
        gross_revenues as net_revenues,

        origin_rate as rate_to_usd,
        gross_revenues * rate_to_usd as gross_revenues_in_usd,
        sales_discount * rate_to_usd as sales_discount_in_usd,
        net_revenues * rate_to_usd as net_revenues_in_usd,

        coalesce(outgoing_user_id, incoming_user_id) as monetized_user_id
    from "CHIPPER"."DBT_TRANSFORMATIONS"."EXPANDED_TRANSFERS" as expanded_transfers
    left join "CHIPPER".{{ var("core_public") }}."TRANSFER_QUOTES" as transfer_quotes
        on expanded_transfers.transfer_id = transfer_quotes.transfer_id
    where
        is_original_transfer_reversed = false
        and exchange_rate_fee_percentage <> 0
        and transfer_type in {{accepted_transfer_types}}
        and transfer_quotes.transfer_id is null
),

forex_fees_using_transfer_quotes as (
    select
        expanded_transfers.transfer_id,
        expanded_transfers.journal_id,
        null as fee_calculation_id,
        null as fee_config_id,
        forex_fee_calculations.id as forex_fee_calculation_id,
        transfer_quotes.id as transfer_quote_id,

        {# /* Null for now, but can be left-joined soon with the transaction_details transformation to populate provider info */ #}
        null as external_provider,
        null as external_provider_transaction_id,

        hlo_created_at as transaction_created_at,
        hlo_updated_at as transaction_updated_at,

        'FOREX_FEES' as revenue_stream,
        forex_fee_calculations.fee_currency as revenue_currency,
        (forex_fee_calculations.fee_percentage / 100) as exchange_rate_fee_percentage_in_decimals,
        null as commission_revenue_rate_in_decimals,
        forex_fee_calculations.debited_amount as gross_revenues,
        null as sales_discount_percentage_in_decimals,
        null as sales_discount,
        gross_revenues as net_revenues,

        origin_rate as rate_to_usd,
        gross_revenues * rate_to_usd as gross_revenues_in_usd,
        sales_discount * rate_to_usd as sales_discount_in_usd,
        net_revenues * rate_to_usd as net_revenues_in_usd,

        coalesce(outgoing_user_id, incoming_user_id) as monetized_user_id
    from "CHIPPER"."DBT_TRANSFORMATIONS"."EXPANDED_TRANSFERS" as expanded_transfers
    inner join "CHIPPER".{{ var("core_public") }}."TRANSFER_QUOTES" as transfer_quotes
        on expanded_transfers.transfer_id = transfer_quotes.transfer_id
    inner join "CHIPPER".{{ var("core_public") }}."FOREX_FEE_CALCULATIONS" as forex_fee_calculations
        on transfer_quotes.id = forex_fee_calculations.transfer_quote_id
    where
        is_original_transfer_reversed = false
        and transfer_type in {{accepted_transfer_types}}
        and origin_amount_in_usd != destination_amount_in_usd
        and forex_fee_calculations.fee_percentage != 0
)

select *
from forex_fees_without_transfer_quotes

union

select *
from forex_fees_using_transfer_quotes
