{{
    config(
        materialized='ephemeral'
    )
}}
SELECT
    uos_amp_user_map.user_id,
    COUNT(DISTINCT ip_address) AS num_distinct_ips
FROM
    {{ ref('uos_amp_user_map') }} uos_amp_user_map
LEFT JOIN {{ ref('uos_pre_onboarding_events') }} uos_pre_onboarding_events
    ON uos_amp_user_map.amplitude_id = uos_pre_onboarding_events.amplitude_id
GROUP BY
    uos_amp_user_map.user_id
