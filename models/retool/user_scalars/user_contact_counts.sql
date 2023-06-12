{{ config(schema='intermediate') }}
SELECT 
    user_id,
    COUNT(DISTINCT CASE WHEN type = 'email' THEN identifier ELSE NULL END) AS num_of_email_identifiers,
    COUNT(DISTINCT CASE WHEN type = 'phone' THEN identifier ELSE NULL END) AS num_of_phone_identifiers
FROM ({{ref("user_contact_info")}})
GROUP BY 1
