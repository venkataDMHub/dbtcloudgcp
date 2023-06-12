{{
    config(
        materialized='ephemeral'
    )
}}

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

{% set time_horizon_in_days = (7, 14, 21, 28, 90, 180) %}
SELECT
    main_party_user_id as user_id,
    {% for key, value in transfer_type_buckets.items() %}
        {% for day_horizon in time_horizon_in_days %}
                SUM(
                    CASE
                        WHEN transfer_type IN {{ value }} 
                        AND CAST(hlo_updated_at AS DATE) > DATEADD(days,-{{ day_horizon }}, CURRENT_TIMESTAMP())
                            THEN 1
                        ELSE 0
                    END
                ) AS {{ key }}_count_past_{{ day_horizon }}_days,
                SUM(
                    CASE
                        WHEN transfer_type IN {{ value }} 
                        AND CAST(hlo_updated_at AS DATE) > DATEADD(days,-{{ day_horizon }}, CURRENT_TIMESTAMP())
                            THEN ABS(ledger_amount_in_usd)
                        ELSE 0
                    END
                ) AS {{ key }}_value_in_usd_past_{{ day_horizon }}_days
                {{ "," if not loop.last }}
            {% endfor %}
        {{ "," if not loop.last }}
    {% endfor %}
FROM
    {{ ref('expanded_ledgers') }}
    WHERE is_original_transfer_reversed = false
    {{ dbt_utils.group_by(
        n = 1
    ) }}
