{{ config(materialized='ephemeral') }}

WITH users_with_transfers AS (
    SELECT
        user_id,
        DATEDIFF(
            'day',
            most_recent_transfer_created_at,
            CURRENT_TIMESTAMP()
        ) AS days_since_last_transfer
    FROM
        chipper.transformed.user_most_recent_transfer_2
),

users_account_age AS (
    SELECT
        id AS user_id,
        created_at AS user_created_at,
        DATEDIFF(
            'day', created_at, CURRENT_TIMESTAMP()
        ) AS account_age_in_days
    FROM chipper.{{var("core_public")}}.users
)


-- user created_at time is UTC time, which may lead to value -1 for column account_age_in_days
-- fixing by excluding those user since it makes little sense to calculate UES for those users 
SELECT
    user_tiers.user_id,
    user_tiers.tier AS user_kyc_tier,
    users_account_age.user_created_at,
    users_account_age.account_age_in_days,
    IFNULL(
        users_with_transfers.days_since_last_transfer, -1
    ) AS days_since_last_transfer,
    IFNULL(
        rt.days_since_last_purchases, -1
    ) AS days_since_last_purchases,
    IFNULL(
        rt.days_since_last_p2p, -1
    ) AS days_since_last_p2p,  
    IFNULL(
        rt.days_since_last_investments, -1
    ) AS days_since_last_investments,  
    IFNULL(
        rt.days_since_last_deposits, -1
    ) AS days_since_last_deposits 
FROM chipper.{{var("compliance_public")}}.user_tiers
LEFT JOIN users_with_transfers 
    ON user_tiers.user_id = users_with_transfers.user_id
LEFT JOIN {{ ref('ues_most_recent_transaction_by_product') }} rt 
    ON user_tiers.user_id = rt.user_id
JOIN users_account_age 
    ON user_tiers.user_id = users_account_age.user_id
WHERE account_age_in_days >= 0
