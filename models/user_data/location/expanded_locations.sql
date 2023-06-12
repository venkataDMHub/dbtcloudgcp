{{  config(
        materialized='incremental',
    ) 
}}
SELECT
    users.id as user_id,
    users.created_at as user_created_at,
    -- first seen sardine location info
    first_seen_sardine.device_id as first_seen_device_id,
    first_seen_sardine.sardine_ip_address as first_sardine_ip_address,
    first_seen_sardine.sardine_ip_city as first_sardine_ip_city,
    first_seen_sardine.sardine_ip_country as first_sardine_ip_country,
    first_seen_sardine.sardine_ip_latitude as first_sardine_ip_latitude,
    first_seen_sardine.sardine_ip_longitude as first_sardine_ip_longitude,
    first_seen_sardine.sardine_ip_region as first_sardine_ip_region,
    first_seen_sardine.risk_level as first_sardine_risk_level,
    -- last seen sardine location info
    last_seen_sardine.device_id as last_seen_device_id,
    last_seen_sardine.sardine_ip_address as last_sardine_ip_address,
    last_seen_sardine.sardine_ip_city as last_sardine_ip_city,
    last_seen_sardine.sardine_ip_country as last_sardine_ip_country,
    last_seen_sardine.sardine_ip_latitude as last_sardine_ip_latitude,
    last_seen_sardine.sardine_ip_longitude as last_sardine_ip_longitude,
    last_seen_sardine.sardine_ip_region as last_sardine_ip_region,
    last_seen_sardine.risk_level as last_sardine_risk_level,
    -- in app provided info
    user_address.city as user_provided_city,
    user_address.country as user_provided_country,
    user_address.lat as user_provided_latitude,
    user_address.long as user_provided_longitude,
    user_address.region as user_provided_region,
    user_address.street as user_provided_street
FROM
    chipper.{{ var('core_public') }}.users
    LEFT JOIN {{ ref('first_seen_sardine_device_ids') }} as first_seen_sardine
        on users.id = first_seen_sardine.user_id
    LEFT JOIN {{ ref('user_provided_address') }} as user_address
        on users.id = user_address.user_id
    -- get last seen sardine device ids info
    LEFT JOIN {{ ref('last_seen_sardine_device_ids') }} as last_seen_sardine
        on users.id = last_seen_sardine.user_id
{% if is_incremental() %}
-- this filter will only be applied on an incremental run
WHERE
    users.created_at >= (select max(user_created_at) from {{ this }})
{% endif %}
