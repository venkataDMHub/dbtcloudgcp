{{ config(
    materialized='table',
    schema='intermediate'
) }}
SELECT
    user_id,
    city,
    country,
    lat,
    long,
    region,
    street
FROM 
    chipper.{{ var('compliance_public') }}.addresses
qualify row_number() over(partition by user_id order by created_at asc) = 1 
