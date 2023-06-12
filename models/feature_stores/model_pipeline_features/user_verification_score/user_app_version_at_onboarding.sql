{{
    config(
        materialized='incremental',
        unique_key='user_id',
        schema='intermediate'
    )
}}

SELECT 
    user_id,
    platform AS os,
    start_version AS app_version
FROM chipper.amplitude.EVENTS_204512
WHERE start_version is not null
{% if is_incremental() %}
    AND user_id not in (SELECT user_id FROM {{ this }})
{% endif %}
QUALIFY ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY client_event_time ASC) = 1
