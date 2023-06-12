SELECT
    referrer_id,
	referrals.id AS referral_id,
    code AS referral_code,
    invited_user_id,
    expanded_users.display_first_name as invited_user_display_first_name,
    expanded_users.display_last_name as invited_user_display_last_name,
    expanded_users.legal_first_name as invited_user_legal_first_name,
    expanded_users.legal_last_name as invited_user_legal_last_name,
    expanded_users.tag as invited_user_tag,
    expanded_users.primary_currency as invited_user_primary_currency,
    expanded_users.kyc_tier as invited_user_kyc_tier,
    expanded_users.created_at as invited_user_created_at,
    referrals.status AS referral_status,
    referrals.created_at AS referral_created_at,
    referrals.updated_at AS referral_updated_at
FROM
	{{var("core_public")}}.referrals
LEFT JOIN
	dbt_transformations.expanded_users ON referrals.invited_user_id = expanded_users.user_id
