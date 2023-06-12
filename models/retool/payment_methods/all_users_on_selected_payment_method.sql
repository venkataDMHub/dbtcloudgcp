SELECT
	DISTINCT expanded_users.user_id,
    expanded_users.legal_first_name,
    expanded_users.legal_last_name,
    expanded_users.tag,
    expanded_users.primary_currency,
    expanded_users.acquisition_source,
    expanded_users.kyc_tier,
    expanded_users.created_at AS user_created_at,
    linked_accounts_with_info.linked_account_info,
    count(expanded_users.user_id) OVER(PARTITION BY linked_account_info) AS num_of_users
FROM {{ ref('expanded_users') }} 
JOIN {{ ref('linked_account_info') }} AS linked_accounts_with_info
ON expanded_users.user_id = linked_accounts_with_info.user_id
