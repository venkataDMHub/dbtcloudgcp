{{ config(materialized = 'ephemeral') }}

With crypto_inflow As (

    Select
        user_id,
        transfer_id,
        transfer_type,
        transfer_created_at,
        origin_currency,
        destination_currency,
        cryptocurrency
    From {{ ref('crypto_details') }}
    Where transfer_type In ('ASSET_TRADES_BUY_SETTLED', 'CRYPTO_DEPOSITS_SETTLED')

),

crypto_outflow As (

    Select
        user_id,
        transfer_id,
        transfer_type,
        transfer_created_at,
        origin_currency,
        destination_currency,
        cryptocurrency
    From {{ ref('crypto_details') }}
    Where transfer_type In ('ASSET_TRADES_SELL_SETTLED', 'CRYPTO_WITHDRAWALS_SETTLED')

),

days_to_next_crypto_outflow As (
    Select
        i.user_id,
        i.transfer_id,
        i.transfer_type,
        i.transfer_created_at,
        i.origin_currency,
        i.destination_currency,
        i.cryptocurrency,
        o.transfer_created_at As next_outflow_transaction_date,
        datediff('days', i.transfer_created_at, o.transfer_created_at) As days_to_next_outflow
    From crypto_inflow As i
    Left Join crypto_outflow As o
        On i.user_id = o.user_id
            And i.cryptocurrency = o.cryptocurrency
            And i.transfer_created_at < o.transfer_created_at
    Qualify days_to_next_outflow = min(days_to_next_outflow) Over(Partition By i.transfer_id)

),

final_crypto As (

    Select
        user_id,
        avg(days_to_next_outflow) as average_days_to_next_crypto_outflow
    From days_to_next_crypto_outflow
    Group By 1

)

Select *
From final_crypto
