{{
    config(
        materialized='ephemeral'
    )
}}
SELECT
    uos_amp_user_map.user_id,
    uos_amp_user_map.amplitude_id,
    event.client_event_time,
    event.event_type,
    event.ip_address,
    event.event_properties:"Completion Time" AS time_to_onboard
FROM
    {{ ref('uos_amp_user_map') }} uos_amp_user_map
LEFT JOIN chipper.amplitude.EVENTS_204512 AS event
    ON uos_amp_user_map.amplitude_id = event.amplitude_id
