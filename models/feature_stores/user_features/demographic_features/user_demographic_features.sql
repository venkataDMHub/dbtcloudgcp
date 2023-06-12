select
    info.user_id,
    info.display_first_name,
    info.display_last_name,
    info.legal_first_name,
    info.legal_last_name,
    info.nationality,
    info.city_of_birth,
    info.dob,
    info.user_age,
    info.gender,
    info.tag,
    info.avatar,
    info.primary_currency,
    info.acquisition_source,
    info.acquisition_date,
    info.kyc_tier,
    info.purpose_of_account,
    info.account_age,
    info.is_internal,
    info.is_deleted,
    info.is_admin,
    info.is_business,
    info.is_valid_user,
    info.invalid_user_reasons,
    info.is_blocked_by_flag,
    info.has_risk_flag,
    info.all_active_flags,
    info.num_flags,
    info.face_urls,
    info.phone_number,
    info.email_address,
    address.country_first,
    address.city_first,
    address.street_first,
    address.postal_code_first,
    address.lat_first,
    address.long_first,
    address.region_first,
    address.country_latest,
    address.city_latest,
    address.street_latest,
    address.postal_code_latest,
    address.lat_latest,
    address.long_latest,
    address.region_latest,
    devices.device_id_first,
    devices.device_model_first,
    devices.os_first,
    devices.app_version_first,
    devices.device_id_latest,
    devices.device_model_latest,
    devices.os_latest,
    devices.app_version_latest,
    uos.uos_score,
    uos.onboarding_time,
    lpv.total_lpv_usd,
    lpv.percent_lpv_from_rewards,
    lpv.lpv_group,
    lpv.lpv_range_min,
    lpv.lpv_range_max,
    ues.last_bucket as latest_engagement_bucket,
    ues.last_score as latest_engagement_score,
    IFF(monetized_user.monetized_user_id IS NOT NULL, TRUE, FALSE) AS is_monetized_user
from {{ ref("user_demographic_basic_info") }} as info
left join {{ ref("user_demographic_address") }} as address on
        info.user_id = address.user_id
left join
    {{ ref("user_demographic_devices") }} as devices on
        info.user_id = devices.user_id
left join {{ ref("user_uos") }} as uos on info.user_id = uos.user_id
left join {{ ref("user_lpv_groups") }} as lpv on info.user_id = lpv.user_id
left join {{ ref("ues_latest_bucket") }} as ues on info.user_id = ues.user_id
LEFT JOIN {{ ref("user_demographic_is_monetized_user") }} AS monetized_user 
ON info.user_id = monetized_user.monetized_user_id
