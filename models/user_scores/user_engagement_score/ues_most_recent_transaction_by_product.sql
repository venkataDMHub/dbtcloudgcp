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

WITH transfer_type_last_timestamp AS (
    SELECT
        main_party_user_id as user_id,
        {% for key, value in transfer_type_buckets.items() %}
        MAX(
            CASE WHEN transfer_type in {{ value }} THEN hlo_updated_at END
        ) as {{ key }}_last_timestamp
        {{ "," if not loop.last }}
        {% endfor %}
    FROM
        {{ ref('time_filtered_settled_transfers') }}
        {{ dbt_utils.group_by(
            n = 1
        ) }}
)

SELECT
    user_id,
    {% for key in transfer_type_buckets.keys() %} 
     DATEDIFF(
        'day', {{ key }}_last_timestamp, CURRENT_TIMESTAMP()
    ) AS days_since_last_{{ key }}
    {{ "," if not loop.last }} 
    {% endfor %}
FROM 
    transfer_type_last_timestamp
