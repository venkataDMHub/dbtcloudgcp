select distinct
    users.id as user_id,
    users.primary_currency,
    users.created_at as user_created_at,
    current_verification.current_tier,
    current_verification.user_verified_at,
    kyc.latest_accepted_kyc_submitted_at,
    kyc_whitelist.whitelisted_kyc_added_at,
    business_users.business_user_verified_at,
    unflagged_timestamp.latest_blocking_flags_unflag_timestamp,
    kyc_revoked_timestamp.user_kyc_revoked_timestamp,
    check_nysm.highest_match_score,
    whitelisted_nysm_added_at,
    blocking_flags.blocking_flags_added_at,
    secondary_document_revoked_timestamp.latest_secondary_kyc_revoked_at,
    check_ng_tier_three.latest_accepted_secondary_kyc_submmitted_at,
    watchlist_latest.updated_at as watchlist_screen_updated_at,
    zeroifnull(kyc.kyc_accepted_doc_count) as kyc_accepted_doc_count,
    coalesce(check_nysm.is_whitelisted_nysm, false) as is_whitelisted_nysm,
    coalesce(blocking_flags.has_blocking_flags, false) as has_blocking_flags,
    coalesce(business_users.has_business_approved, false)
        as has_business_approved,
    coalesce(selfie.has_selfie_checked, false) as has_selfie_checked,
    coalesce(uos_approval.has_uos_manually_checked, false)
        as has_uos_manually_checked,
    coalesce(watchlist_latest.has_watchlist_flag, false) as has_watchlist_flag,
    coalesce(kyc_whitelist.is_whitelisted_kyc, false) as is_whitelisted_kyc,
    coalesce(
        check_ng_tier_three.has_verified_address, false
    ) as has_verified_address,
    coalesce(
        check_ng_tier_three.has_approved_secondary_document, false
    ) as has_approved_secondary_document

from {{ var("core_public") }}.users
left join
    {{ ref("user_current_verification") }} as current_verification
    on users.id = current_verification.user_id
left join
    {{ ref("check_blocking_flags") }} as blocking_flags
    on users.id = blocking_flags.user_id
left join
    {{ ref("check_business_users") }} as business_users
    on users.id = business_users.user_id
left join {{ ref("check_nysm") }} as check_nysm on users.id = check_nysm.user_id
left join {{ ref("check_selfie") }} as selfie on users.id = selfie.user_id
left join
    {{ ref("check_uos_approval") }} as uos_approval
    on users.id = uos_approval.user_id
left join
    {{ ref("check_watchlist_latest") }} as watchlist_latest
    on users.id = watchlist_latest.user_id
left join {{ ref("user_kyc") }} as kyc on users.id = kyc.user_id
left join {{ ref("user_latest_uos") }} as uos on users.id = uos.user_id
left join
    {{ ref("check_kyc_whitelist_users") }} as kyc_whitelist
    on users.id = kyc_whitelist.user_id
left join
    {{ ref("blocking_flags_unflagged_timestamp") }} as unflagged_timestamp
    on users.id = unflagged_timestamp.user_id
left join
    {{ ref("user_kyc_revoked_timestamp") }} as kyc_revoked_timestamp
    on users.id = kyc_revoked_timestamp.user_id
left join
    {{ ref("check_ng_tier_three") }} as check_ng_tier_three
    on users.id = check_ng_tier_three.user_id
left join
    {{ ref("check_ng_tier_secondary_revoked_timestamp") }}
        as secondary_document_revoked_timestamp
    on users.id = secondary_document_revoked_timestamp.user_id
