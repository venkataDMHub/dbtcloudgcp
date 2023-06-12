{{
    config(
        materialized='ephemeral'
    )
}}
WITH rank_events AS (
    SELECT
        user_id,
        amplitude_id,
        client_event_time,
        event_type,
        ip_address,
        ROW_NUMBER() OVER(PARTITION BY amplitude_id ORDER BY client_event_time) AS row_number
    FROM
        {{ ref('uos_user_events') }} uos_user_events
),

onboarding_ended_event AS (
    SELECT
        amplitude_id,
        row_number
    FROM
        rank_events
    WHERE
        event_type = 'Onboarding - Ended'
)

SELECT
    rank_events.user_id,
    rank_events.amplitude_id,
    rank_events.client_event_time,
    rank_events.event_type,
    rank_events.ip_address
FROM
    rank_events
LEFT JOIN onboarding_ended_event
    ON rank_events.amplitude_id = onboarding_ended_event.amplitude_id
WHERE
    rank_events.row_number <= onboarding_ended_event.row_number
