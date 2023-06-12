{{
    config(
        materialized='incremental',
        unique_key='user_id',
        schema='intermediate'
    )
}}

WITH latest_uos AS (
    SELECT
        user_id,
        uos_score,
        uos_created_at
    FROM
        utils.user_onboarding_score
    QUALIFY
        ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY uos_created_at DESC)
        = 1
)

SELECT
    user_id,
    uos_score,
    uos_created_at
FROM latest_uos
{% if is_incremental() %}
    WHERE
        uos_created_at > (SELECT MAX(uos_created_at) FROM {{ this }})
        AND user_id NOT IN (SELECT DISTINCT user_id FROM {{ this }})
{% endif %}
