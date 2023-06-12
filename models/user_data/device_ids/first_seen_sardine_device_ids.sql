{{ config(materialized='ephemeral') }}

SELECT 
    user_id,
    device_id,
    device_model,
    risk_level,
    sardine_ip_address,
    sardine_ip_city,
    sardine_ip_country,
    sardine_ip_latitude,
    sardine_ip_longitude,
    sardine_ip_region  
FROM 
    {{ ref("ranked_sardine_device_ids") }}
WHERE 
    row_num_asc = 1
