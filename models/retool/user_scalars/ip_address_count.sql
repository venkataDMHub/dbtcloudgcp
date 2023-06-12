{{ config(schema='intermediate') }}
SELECT
    user_id,
    COUNT(DISTINCT ip_address) AS num_of_ip_addresses
FROM {{var("core_public")}}.user_ip_addresses
GROUP BY 1
