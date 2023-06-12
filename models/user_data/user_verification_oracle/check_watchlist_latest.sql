{{
    config(
        materialized='incremental',
        unique_key='user_id',
        schema='intermediate'
    )
}}

SELECT
    user_id,
    TRUE AS has_watchlist_flag,
    updated_at
FROM
    {{ var("compliance_public") }}.watchlist_checks
WHERE
    type = 'SANCTIONS_PEP'
    AND provider != 'OPEN_SANCTIONS_RESCREEN'
    {% if is_incremental() %}
        AND updated_at > (SELECT MAX(updated_at) FROM {{ this }})

    {% endif %}
QUALIFY
    ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY updated_at DESC) = 1
