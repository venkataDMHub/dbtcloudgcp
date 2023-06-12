{{
    config(
        materialized='incremental',
        unique_key='user_id',
        schema='intermediate'
    )
}}

SELECT
    user_id,
    tier AS current_tier,
    updated_at AS user_verified_at
FROM
    {{ var('compliance_public') }}.user_tiers
{% if is_incremental() %}
    WHERE user_verified_at > (SELECT MAX(user_verified_at) FROM {{ this }})
{% endif %}
QUALIFY
    ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY user_verified_at DESC) = 1
