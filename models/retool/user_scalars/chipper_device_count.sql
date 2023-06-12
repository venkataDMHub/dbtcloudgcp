{{ config(schema='intermediate') }}
SELECT
    user_id,
    COUNT(DISTINCT device_id) AS num_of_chipper_devices
FROM {{var("core_public")}}.user_device_ids
GROUP BY 1
