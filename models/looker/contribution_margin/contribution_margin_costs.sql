{{ config(materialized="table", schema="looker") }}

with
    cico as (
        select
            transfer_id,
            null as provider_transaction_id,
            transaction_settled_date,
            margin_stream,
            journal_type,
            transfer_type,
            external_provider,
            user_id,
            case
                when margin_stream = 'CASH_OUT'
                then destination_currency
                when margin_stream = 'CASH_IN'
                then origin_currency
            end as currency,
            case
                when margin_stream = 'CASH_OUT'
                then destination_amount
                when margin_stream = 'CASH_IN'
                then origin_amount
            end as transaction_amount,
            concat(external_provider, '_', margin_stream, '_', 'FEE') as fee_type,
            fee as fee_amount,
            cogs_in_usd as fee_amount_in_usd,
            cogs_in_usd_parallel as fee_amount_in_usd_parallel
        
        from 
            {{ ref("cico_contribution_margin") }}
    ),

    cards as (
        select
            transfer_id,
            provider_transaction_id,
            transaction_settled_date,
            margin_stream,
            journal_type,
            transfer_type,
            external_provider,
            user_id,
            card_currency as currency,
            card_transaction_amount as transaction_amount,
            fee_type,
            fee_amount,
            fee_amount as fee_amount_in_usd,
            fee_amount_in_usd as fee_amount_in_usd_parallel
        
        from
            {{ ref("card_transactions_contribution_margin") }} unpivot (
                fee_amount
                for fee_type
                in (card_signup_fee, active_user_fee, decline_fee, transaction_fee)
            ) as unpiv
    ),

    crypto as (
        select
            transfer_id,
            null as provider_transaction_id,
            transaction_settled_date,
            margin_stream,
            journal_type,
            transfer_type,
            external_provider,
            user_id,
            case
                when transfer_type = 'ASSET_TRADES_BUY_SETTLED'
                then origin_currency
                when transfer_type = 'ASSET_TRADES_SELL_SETTLED'
                then destination_currency
            end as currency,
            origin_amount as transaction_amount,
            'CRYPTO_DESTINATION_AMOUNT' as fee_type,
            destination_amount as fee_amount,
            cogs_in_usd as fee_amount_in_usd,
            cogs_in_usd_parallel as fee_amount_in_usd_parallel
        
        from 
            {{ ref("crypto_contribution_margin") }}
    ),

    stocks as (
        select
            transfer_id,
            null as provider_transaction_id,
            transaction_settled_date,
            margin_stream,
            journal_type,
            transfer_type,
            external_provider,
            user_id,
            origin_currency as currency,
            origin_amount as transaction_amount,
            'MONTHLY_DRIVEWEALTH_FEE' as fee_type,
            cogs as fee_amount,
            cogs_in_usd as fee_amount_in_usd,
            cogs_in_usd_parallel as fee_amount_in_usd_parallel
            
        from
            {{ ref("stocks_contribution_margin") }} 

    ),

    airtime as (
        select
            transfer_id,
            null as provider_transaction_id,
            transaction_settled_date,
            margin_stream,
            journal_type,
            transfer_type,
            external_provider,
            user_id,
            origin_currency as currency,
            receive_value as transaction_amount,
            'AIRTIME_COMMISSION_COST' as fee_type,
            cogs as fee_amount,
            cogs_in_usd as fee_amount_in_usd,
            cogs_in_usd_parallel as fee_amount_in_usd_parallel
            
        from
            {{ ref("airtime_contribution_margin") }} 

    ),

    cm_costs as (

        select * from cico
        union all
        select * from cards
        union all
        select * from crypto
        union all
        select * from stocks
        union all
        select * from airtime

    )

select dense_rank() over (
    order by 
        transfer_id,
        provider_transaction_id,
        transaction_settled_date,
        margin_stream,
        journal_type,
        transfer_type,
        external_provider,
        user_id,
        currency,
        transaction_amount,
        fee_type,
        fee_amount,
        fee_amount_in_usd,
        fee_amount_in_usd_parallel
    )   as row_number,
    *
from cm_costs
order by row_number
