WITH associated_user_ids AS (
    SELECT 
        linked_account_info AS linked_account_info_a,
        count(distinct user_id) AS associated_user_ids 
    FROM {{ ref('linked_account_info') }}
    GROUP BY linked_account_info
)
SELECT
    user_id,
    linked_account_id,
    linked,
    verified,
    external,
    associated_user_ids,
    type,
    linked_account_info,
    linked_account_created_at,
    marked_as_malicious
FROM
    {{ ref('linked_account_info') }} AS payment_method_info
LEFT JOIN associated_user_ids 
ON payment_method_info.linked_account_info = associated_user_ids.linked_account_info_a
