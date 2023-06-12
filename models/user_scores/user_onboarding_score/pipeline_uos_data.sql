SELECT
    distinct
    uos_amp_user_map.user_id,
    uos_amp_user_map.first_name,
    uos_amp_user_map.last_name,
    uos_amp_user_map.user_created_at,
    uos_onboarding_time.time_to_onboard,
    uos_onboarding_time.onboarding_completion_timestamp,
    uos_feature_distinct_ip.num_distinct_ips,
    user_info.dob,
    user_info.nationality,
    user_info.purpose_of_account,
    latest_street.street,
    COALESCE(uos_feature_users_per_ip.user_per_ip_count, 0) AS user_per_ip_count,
    DATE_TRUNC('MONTH', TO_DATE(uos_onboarding_time.onboarding_completion_timestamp)) AS year_month

FROM
    {{ ref('uos_amp_user_map') }} AS uos_amp_user_map
LEFT JOIN {{ ref('uos_onboarding_time') }} AS uos_onboarding_time
    ON uos_amp_user_map.user_id = uos_onboarding_time.user_id
LEFT JOIN {{ ref('uos_feature_distinct_ip') }} AS uos_feature_distinct_ip
    ON uos_amp_user_map.user_id = uos_feature_distinct_ip.user_id
LEFT JOIN {{ ref('uos_feature_users_per_ip') }} AS uos_feature_users_per_ip
    ON uos_amp_user_map.user_id = uos_feature_users_per_ip.user_id
LEFT JOIN chipper.{{ var('compliance_public') }}.user_info
    ON uos_amp_user_map.user_id = user_info.user_id
LEFT JOIN {{ ref('uos_latest_street') }} AS latest_street
    ON uos_amp_user_map.user_id = latest_street.user_id
WHERE
    uos_onboarding_time.time_to_onboard IS NOT NULL
