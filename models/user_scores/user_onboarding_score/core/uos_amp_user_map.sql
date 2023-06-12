SELECT
    users.user_id,
    users.first_name,
    users.last_name,
    users.user_created_at,
    map_user_amplitude.amplitude_id as amplitude_id
FROM
    {{ ref('uos_users_not_scored') }} AS users
LEFT JOIN {{ ref('final_chipper_amplitude_mapping') }} map_user_amplitude
    ON users.user_id = map_user_amplitude.user_id
