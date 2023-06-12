{{
    config(
        materialized='ephemeral'
    )
}}
SELECT
    user_id,
    MAX(time_to_onboard) AS time_to_onboard,
    MAX(client_event_time) AS onboarding_completion_timestamp
FROM
    {{ ref('uos_user_events') }}
WHERE
    event_type = 'Onboarding - Ended'
GROUP BY
    user_id
