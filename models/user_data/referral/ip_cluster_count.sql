{{
    config(
        materialized='view',
    )
}}
WITH ip_cluster_count AS (
    SELECT
        referrals.referrer_id as user_id,
        CASE
            when ip_address IS NULL THEN 0
            else count(DISTINCT referrals.id) over(PARTITION by referrals.referrer_id, user_ip_addresses.ip_address)
        END AS ip_cluster_count
    FROM 
        {{ var('core_public') }}.referrals
        JOIN {{ var('core_public' )}}.user_ip_addresses 
            ON referrals.invited_user_id = user_ip_addresses.user_id
)
SELECT
    user_id,
    max(ip_cluster_count) as max_ip_cluster_count
FROM 
    ip_cluster_count
GROUP BY 
    user_id
