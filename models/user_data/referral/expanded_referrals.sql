{{
    config(
        materialized='incremental',
        unique_key='id'
    )
}}

SELECT
    referrals.id,
    -- referrer information
    referrals.referrer_id,
    expanded_referrer.display_first_name AS referrer_display_first_name,
    expanded_referrer.display_last_name AS referrer_display_last_name,
    expanded_referrer.tag AS referrer_tag,
    expanded_referrer.email_address AS referrer_email,
    expanded_referrer.phone_number AS referrer_phone_number,
    expanded_referrer.kyc_tier AS referrer_kyc_tier,
    expanded_referrer.num_flags AS referrer_num_flags,
    expanded_referrer.all_active_flags AS referrer_all_active_flags,
    expanded_referrer.created_at AS referrer_created_at,
    referrer_selfie.face_url AS referrer_selfie_url,
    -- additional fraud info
    referrer_device.device_id AS referrer_device_id,
    referrer_device.device_model AS referrer_device_model,
    referrer_device.risk_level AS referrer_sardine_risk_level,
    referrer_uos.uos_score AS referrer_uos_score,
    referrer_uos.uos_score_v3 AS referrer_uos_score_v3,
    referrals.invited_user_id,
    -- invited user information
    expanded_invited.display_first_name AS invited_display_first_name,
    expanded_invited.display_last_name AS invited_display_last_name,
    expanded_invited.tag AS invited_tag,
    expanded_invited.email_address AS invited_email,
    expanded_invited.phone_number AS invited_phone_number,
    expanded_invited.num_flags AS invited_num_flags,
    expanded_invited.all_active_flags AS invited_all_active_flags,
    expanded_invited.kyc_tier AS invited_kyc_tier,
    expanded_invited.created_at AS invited_created_at,
    invited_selfie.face_url AS invited_selfie_url,
    invited_device.device_id AS invited_device_id,
    invited_device.device_model AS invited_device_model,
    -- additional fraud info
    invited_device.risk_level AS invited_sardine_risk_level,
    invited_uos.uos_score AS invited_uos_score,
    invited_uos.uos_score_v3 AS invited_uos_score_v3,
    referrals.status,
    referrals.last_error,
    -- general referral information
    referrals.created_at AS referral_created_at,
    referrals.updated_at AS referral_updated_at,
    ip_cluster_count.max_ip_cluster_count,
    CASE
        WHEN referrer_similar_selfie.user_id IS NULL THEN False
        ELSE True
    END AS referrer_user_has_similar_selfie,
    -- referral level stats
    CASE
        WHEN invited_similar_selfie.user_id IS NULL THEN False
        ELSE True
    END AS invited_user_has_similar_selfie,
    count(DISTINCT referrals.id) OVER (PARTITION BY referrals.referrer_id) AS referrals_made,
    count(
        DISTINCT referrals.id
    ) OVER (PARTITION BY referrals.referrer_id, referrals.status) AS referrals_made_by_status,
    -- manual review stats
    iff(whitelisted_referrals.referral_id IS NULL, False, True) AS is_manually_reviewed
FROM
    chipper.{{ var("core_public") }}.referrals
LEFT JOIN {{ ref('expanded_users') }} AS expanded_referrer
    ON referrals.referrer_id = expanded_referrer.user_id
LEFT JOIN {{ ref('expanded_users') }} AS expanded_invited
    ON referrals.invited_user_id = expanded_invited.user_id
LEFT JOIN {{ ref("user_best_selfie") }} AS referrer_selfie
    ON referrals.referrer_id = referrer_selfie.user_id
LEFT JOIN {{ ref("user_best_selfie") }} AS invited_selfie
    ON referrals.invited_user_id = invited_selfie.user_id
LEFT JOIN {{ ref("last_seen_sardine_device_ids") }} AS referrer_device
    ON referrals.referrer_id = referrer_device.user_id
LEFT JOIN {{ ref("last_seen_sardine_device_ids") }} AS invited_device
    ON referrals.invited_user_id = invited_device.user_id
LEFT JOIN {{ ref("user_uos") }} AS referrer_uos
    ON referrals.referrer_id = referrer_uos.user_id
LEFT JOIN {{ ref("user_uos") }} AS invited_uos
    ON referrals.invited_user_id = invited_uos.user_id
LEFT JOIN {{ ref("smile_similar_selfie") }} AS referrer_similar_selfie
    ON referrals.referrer_id = referrer_similar_selfie.user_id
LEFT JOIN {{ ref("smile_similar_selfie") }} AS invited_similar_selfie
    ON referrals.invited_user_id = invited_similar_selfie.user_id
LEFT JOIN {{ ref("ip_cluster_count") }}
    ON referrals.referrer_id = ip_cluster_count.user_id
LEFT JOIN {{ ref("whitelisted_referrals") }} AS whitelisted_referrals
    ON referrals.id = whitelisted_referrals.referral_id

{% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    WHERE referral_updated_at > (SELECT max(referral_updated_at) FROM {{ this }})
{% endif %}
