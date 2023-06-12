{{
    config(
        materialized='ephemeral'
    )
}}

{% set BLOCKED_FLAGS=('USER_LOCKED', 'USER_OFFBOARDED', 'BLOCKED_PEP', 'POTENTIAL_SANCTIONS_MATCH', 'CONFIRMED_SANCTIONS_MATCH') %}

SELECT
    distinct user_id,
    max(date_unflagged) as latest_blocking_flags_unflag_timestamp
FROM
    {{ var("compliance_public") }}.account_flags
WHERE
    flag IN {{ BLOCKED_FLAGS }}
    AND date_unflagged is not NULL
{{ dbt_utils.group_by(n=1) }}
