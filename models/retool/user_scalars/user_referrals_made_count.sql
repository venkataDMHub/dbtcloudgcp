{{ config(schema='intermediate') }}
SELECT 
    referrer_id AS user_id,
    COUNT(DISTINCT invited_user_id) AS num_of_referrals
FROM ({{ref("user_referrals_made")}})
GROUP BY 1
