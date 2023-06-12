{{ config(schema='intermediate') }}
SELECT
    user_id,
    COUNT(DISTINCT linked_account_info) AS num_of_payment_methods
FROM ({{ref("linked_account_info")}})
GROUP BY 1
