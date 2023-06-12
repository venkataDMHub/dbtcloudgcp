{{
    config(
        materialized='ephemeral'
    )
}}

SELECT
    distinct user_id,
    TRUE as has_business_approved,
    max(date_flagged) as business_user_verified_at
FROM
    {{ var("compliance_public") }}.account_flags
WHERE
    flag in ('KYB_VERIFIED')
    AND date_unflagged is NULL
{{ dbt_utils.group_by(n=2) }}
