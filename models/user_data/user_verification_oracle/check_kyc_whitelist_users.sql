{{
    config(
        materialized='ephemeral'
    )
}}


SELECT
    distinct user_id,
    TRUE as is_whitelisted_kyc,
    max(date_flagged) as whitelisted_kyc_added_at
FROM
    {{ var("compliance_public") }}.account_flags
WHERE
    flag  = 'WHITELISTED_KYC'
    AND date_unflagged is NULL
{{ dbt_utils.group_by(n=2) }}
