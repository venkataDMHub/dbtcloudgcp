SELECT 
    DISTINCT linked_account_id,
    type,
    linked_account_info,
    linked_account_created_at,
    is_linked,
    is_verified,
    linked,
    verified,
    marked_as_malicious,
    external,
    linked_accounts_with_info.user_id,
    expanded_users.display_first_name,
    expanded_users.display_last_name,
    expanded_users.legal_first_name,
    expanded_users.legal_last_name,
    expanded_users.tag,
    expanded_users.primary_currency,
    expanded_users.acquisition_source,
    expanded_users.kyc_tier,
    expanded_users.created_at AS user_created_at,
    count(linked_account_id) OVER(PARTITION BY linked_account_info) AS num_of_linked_accounts
FROM {{ ref("linked_account_info") }} AS linked_accounts_with_info
LEFT JOIN
    {{ ref("expanded_users") }}
    ON linked_accounts_with_info.user_id = expanded_users.user_id
