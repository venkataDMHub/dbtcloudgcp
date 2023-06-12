{% set transaction_aggregates_cols = [
    "purchases_transaction_count", 
    "purchases_transaction_value_in_usd", 
    "p2p_transaction_count", 
    "p2p_transaction_value_in_usd", 
    "investments_transaction_count", 
    "investments_transaction_value_in_usd", 
    "deposits_transaction_count", 
    "deposits_transaction_value_in_usd", 
] %}

WITH ues_all_features AS (
    SELECT
        ues_user_transaction_timings.user_id,
        ues_user_transaction_timings.user_kyc_tier,
        ues_user_transaction_timings.user_created_at,
        ues_user_transaction_timings.days_since_last_transfer,
        ues_user_transaction_timings.days_since_last_purchases,
        ues_user_transaction_timings.days_since_last_p2p,
        ues_user_transaction_timings.days_since_last_investments,
        ues_user_transaction_timings.days_since_last_deposits,
        ues_user_transaction_timings.account_age_in_days,
        
        {% for col in transaction_aggregates_cols %}
        IFNULL(
            ues_transaction_aggregates.{{ col }},
            0
        ) as {{ col }},
            {% endfor %} 
        IFNULL(ues_amp_features.investments_amp_count, 0) AS investments_amp_count
    FROM {{ ref('ues_user_transaction_timings') }} as ues_user_transaction_timings
    LEFT JOIN {{ ref('ues_transaction_aggregates') }} as ues_transaction_aggregates
        ON ues_user_transaction_timings.user_id = ues_transaction_aggregates.user_id
    LEFT JOIN {{ ref('ues_amp_features') }} as ues_amp_features
        ON ues_user_transaction_timings.user_id = ues_amp_features.user_id
)


SELECT
    user_id,
    user_kyc_tier,
    user_created_at,
    days_since_last_transfer,
    account_age_in_days,
    purchases_transaction_count,
    purchases_transaction_value_in_usd,
    days_since_last_purchases,
    p2p_transaction_count,
    p2p_transaction_value_in_usd,
    days_since_last_p2p,
    investments_transaction_count,
    investments_transaction_value_in_usd,
    investments_amp_count,
    days_since_last_investments,
    deposits_transaction_count,
    deposits_transaction_value_in_usd,
    days_since_last_deposits,
    (
        CASE
            -- Inactive case 1: users made 0 settled transactions in the time window and never checked amplitude related pages
            WHEN 
                days_since_last_transfer > ABS({{var("ues_time_horizon_in_days")}}) and investments_amp_count = 0 THEN FALSE
            -- Inactive case 2: users never made any settled transactions and never checked amplitude related pages
            WHEN 
                days_since_last_transfer = -1 and investments_amp_count = 0 THEN FALSE
            ELSE TRUE
        END) AS is_active
FROM ues_all_features
WHERE user_id not in ({{internal_users()}})
  and user_id not in ({{business_users()}})
