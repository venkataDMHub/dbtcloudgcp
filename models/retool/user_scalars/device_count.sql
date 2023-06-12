{{ config(schema = 'intermediate') }}
SELECT user_id,
  COUNT(DISTINCT device_id) AS num_of_devices
FROM {{ var("core_public") }}.device_fingerprints
GROUP BY 1
