{{
    config(
        materialized='ephemeral'
    )
}}

SELECT DISTINCT
    user_id,
    TRUE AS has_uos_manually_checked
FROM
    {{ var("compliance_public") }}.account_flags
WHERE
    flag IN ('FLAGGED_BY_UOS')
    AND date_unflagged IS NOT NULL
