WITH dedup_business_info AS (
	SELECT * 
	FROM {{ var("compliance_public") }}.business_info
	QUALIFY MAX(business_info.updated_at) OVER (PARTITION BY primary_account_owner_id) = business_info.updated_at
)
SELECT
    expanded_users.user_id,
    expanded_users.avatar,
    expanded_users.display_first_name,
    expanded_users.display_last_name,
    expanded_users.legal_first_name,
    expanded_users.legal_last_name,
    expanded_users.tag,
    expanded_users.primary_currency,
    expanded_users.acquisition_source,
    expanded_users.kyc_tier,
    expanded_users.created_at,
    expanded_users.phone_number,
    expanded_users.email_address,
    expanded_users.is_deleted,
    expanded_users.is_business,
    expanded_users.flag,
    expanded_users.all_active_flags,
    expanded_users.num_flags,
    expanded_users.is_blocked_by_flag,
    expanded_users.has_risk_flag,
    dedup_business_info.name as business_name,
    dedup_business_info.dba as doing_business_as,
    dedup_business_info.country as business_country,
    dedup_business_info.industry,
    dedup_business_info.registration_status as business_registration_status,
    dedup_business_info.type as business_registration_type,
    dedup_business_info.status as chipper_business_account_status
FROM
	{{ ref('expanded_users') }} 
LEFT JOIN
     dedup_business_info ON expanded_users.user_id = dedup_business_info.primary_account_owner_id
