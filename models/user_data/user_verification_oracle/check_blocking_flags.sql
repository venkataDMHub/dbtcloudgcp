{{
    config(
        materialized='ephemeral'
    )
}}

{% set BLOCKED_FLAGS=('USER_LOCKED', 'USER_OFFBOARDED', 'BLOCKED_PEP', 'POTENTIAL_SANCTIONS_MATCH', 'CONFIRMED_SANCTIONS_MATCH') %}

SELECT
    distinct user_id,
    TRUE as has_blocking_flags,
    max(date_flagged) as blocking_flags_added_at
FROM
    {{ var("compliance_public") }}.account_flags
WHERE
    flag IN {{ BLOCKED_FLAGS }}
    AND date_unflagged is NULL
{{ dbt_utils.group_by(n=2) }}
