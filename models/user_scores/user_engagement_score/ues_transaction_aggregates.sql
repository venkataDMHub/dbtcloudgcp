{{ config(materialized='ephemeral') }}

{% set transfer_type_buckets ={ 'purchases': (
    'AIRTIME_PURCHASES_COMPLETED', 
    'BILL_PAYMENTS_COMPLETED' 
    'DATA_PURCHASES_COMPLETED', 
    'NETWORK_API_C2B_SETTLED' 
),
'p2p': (
    'PAYMENTS_P2P_SETTLED', 
    'REQUESTS_SETTLED',
    'PAYMENT_INVITATIONS_SETTLED'
),
'investments': (
    'ASSET_TRADES_BUY_SETTLED',
    'ASSET_TRADES_SELL_SETTLED',
    'STOCK_TRADES_BUY_SETTLED',
    'STOCK_TRADES_SELL_SETTLED',
),
'deposits': (
    'DEPOSITS_SETTLED',
    'ISSUED_CARD_TRANSACTIONS_FUNDING_COMPLETED',
    'CRYPTO_DEPOSITS_SETTLED'
) } %}

SELECT
    main_party_user_id as user_id,
    {% for key,
        value in transfer_type_buckets.items() %}
        SUM(
            CASE
                WHEN transfer_type IN {{ value }} THEN 1
                ELSE 0
            END
        ) AS {{ key }}_transaction_count,
        SUM(
            CASE
                WHEN transfer_type IN {{ value }} THEN ABS(ledger_amount_in_usd)
                ELSE 0
            END
        ) AS {{ key }}_transaction_value_in_usd 
        {{ "," if not loop.last }}
    {% endfor %}
FROM
    {{ ref('time_filtered_settled_transfers') }}
    {{ dbt_utils.group_by(
        n = 1
    ) }}
