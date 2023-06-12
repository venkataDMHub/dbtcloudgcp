{{
    config(
        materialized='ephemeral'
    )
}}
WITH agg_ip_data AS (
    SELECT
        ip_address,
        COUNT(DISTINCT user_id) AS user_per_ip_count
    FROM
        chipper.{{ var('core_public') }}.user_ip_addresses
    GROUP BY
        ip_address
)

SELECT
    pre_onboard_events.user_id,
    MAX(user_per_ip_count) AS user_per_ip_count
FROM
    {{ ref('uos_pre_onboarding_events') }} AS pre_onboard_events
LEFT JOIN agg_ip_data
    ON pre_onboard_events.ip_address = agg_ip_data.ip_address
GROUP BY
    pre_onboard_events.user_id
