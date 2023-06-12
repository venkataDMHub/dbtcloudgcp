{{ config(materialized='ephemeral') }}

{% set amp_events_bucket = {
'investments' : (
    'Crypto - Price Screen Viewed',
    'Stocks - Price Screen Viewed'
    )
} %}

{% set amp_events = ('Crypto - Price Screen Viewed', 
                     'Stocks - Price Screen Viewed') %}

SELECT
    user_id,
    {% for key, value in amp_events_bucket.items() %}
    SUM(
        CASE WHEN event_type in {{ value }} THEN 1 ELSE 0 END
    ) as {{ key }}_amp_count
    {{ "," if not loop.last }}
    {% endfor %}
FROM
    chipper.amplitude.events_204512 AS amp_event
WHERE
    CAST(
        client_event_time AS DATE
    ) > DATEADD(
        days,
        {{var("ues_time_horizon_in_days")}},
        CURRENT_TIMESTAMP()
    ) 
AND user_id IS NOT NULL
AND event_type IN {{ amp_events }}
{{ dbt_utils.group_by(
    n = 1
) }}
