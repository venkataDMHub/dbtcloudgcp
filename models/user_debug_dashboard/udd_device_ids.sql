WITH user_device_history AS (
    SELECT
        udi.user_id,
        u.tag,
        udi.device_id
    FROM
        chipper.{{ var('core_public') }}.user_device_ids AS udi
    LEFT JOIN chipper.{{ var('core_public') }}.users AS u
        ON u.id = udi.user_id
)

SELECT
    user_device_history.user_id AS original_user_id,
    user_device_history.tag AS original_user_tag,
    udi.device_id,
    udi.user_id,
    udi.created_at AS user_device_created_at,
    udi.updated_at AS user_device_updated_at,
    udi.most_recent_ip AS user_device_most_recent_ip,
    udi.app_version,
    udi.os_version,
    udi.device_type,
    udi.carrier,
    RANK() OVER(PARTITION BY udi.device_id, udi.user_id ORDER BY udi.updated_at DESC) AS login_rank
FROM
    user_device_history
LEFT JOIN chipper.{{ var('core_public') }}.user_device_ids AS udi
    ON user_device_history.device_id = udi.device_id
