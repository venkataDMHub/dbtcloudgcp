{{ config(materialized='ephemeral') }}

WITH sardine_devices AS (
    SELECT
        user_id,
        device_id,
        provider_response,
        ROW_NUMBER() OVER(PARTITION BY USER_ID ORDER BY TIMESTAMP ASC) AS row_num_asc,
        ROW_NUMBER() OVER(PARTITION BY USER_ID ORDER BY TIMESTAMP DESC) AS row_num_desc
    FROM
        chipper.{{ var("core_public") }}.device_fingerprints

)
SELECT 
    user_id,
    device_id,
    TRIM(provider_response:attributes:Model) AS device_model,
    IFNULL(TRIM(provider_response:level), 'N/A') AS risk_level,
    -- ip and location level information
    TRIM(provider_response:ipAddresses:v4) as sardine_ip_address,
    TRIM(provider_response:ipLocation:city) as sardine_ip_city,
    TRIM(provider_response:ipLocation:country) as sardine_ip_country,
    TRIM(provider_response:ipLocation:latitude) as sardine_ip_latitude,
    TRIM(provider_response:ipLocation:longitude) as sardine_ip_longitude,
    TRIM(provider_response:ipLocation:region) as sardine_ip_region,
    row_num_asc,
    row_num_desc
FROM 
    sardine_devices
