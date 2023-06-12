{{
    config(
        materialized='incremental',
        unique_key='user_id',
        schema='intermediate'
    )
}}

with user_kyc_submission_time AS (
    SELECT 
        owner_id AS user_id,
        MIN(submitted_at) AS first_kyc_submission_at
    FROM {{var("compliance_public")}}.kyc_documents
    GROUP BY user_id
)

SELECT 
    user_tiers.user_id,
    users.created_at as user_created_at,
    user_uos.onboarding_time as onboarding_time_sec,
    datediff(second, users.created_at, user_kyc_submission_time.first_kyc_submission_at) as time_creation_to_first_kyc_submission_sec,
    datediff(second, users.created_at, user_tiers.updated_at) as time_creation_to_verified_sec
FROM {{var("compliance_public")}}.user_tiers
    LEFT JOIN {{var("core_public")}}.users ON user_tiers.user_id =  users.id
    LEFT JOIN {{ ref('user_uos')}} AS user_uos ON user_tiers.user_id = user_uos.user_id
    LEFT JOIN user_kyc_submission_time ON user_tiers.user_id = user_kyc_submission_time.user_id
WHERE 
    user_tiers.tier in ({{ verified_tiers() }})
{% if is_incremental() %}
    AND users.created_at > (SELECT MAX(user_created_at) FROM {{ this }})
{% endif %}
