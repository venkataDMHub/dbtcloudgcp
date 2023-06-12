-- For example,
-- 6ab31ce0-0b29-11eb-b5eb-0b858df3d004, opumbi, 949d8890-e52a-11e9-9dd2-c978258c29de, lewinsky, 9
-- are the user_id, user_tag, user_referred_by, user_referred_by_tag, user_referred_by_referral_count
WITH user_referrer AS (
    SELECT DISTINCT
    
        r.invited_user_id AS user_id,
        r.referrer_id AS user_referred_by,
        r.status AS referral_status
    FROM
        chipper.{{ var('core_public') }}.referrals AS r
),

sibiling_referrals AS (
    SELECT
        ur.user_referred_by,
        COUNT(DISTINCT r.invited_user_id) AS user_referred_by_referral_count
    FROM
        user_referrer AS ur
    LEFT JOIN chipper.{{ var('core_public') }}.referrals AS r
        ON ur.user_referred_by = r.referrer_id
    WHERE
        r.status = 'SETTLED'
    GROUP BY
        ur.user_referred_by
)

SELECT
    u.id AS user_id,
    u.tag AS user_tag,
    ur.referral_status,
    ur.user_referred_by AS user_referred_by,
    u_ur.tag AS user_referred_by_tag,
    IFNULL(sr.user_referred_by_referral_count, 0) AS user_referred_by_referral_count
FROM
    chipper.{{ var('core_public') }}.users AS u
LEFT JOIN user_referrer AS ur
    ON u.id = ur.user_id
LEFT JOIN sibiling_referrals AS sr
    ON ur.user_referred_by = sr.user_referred_by
LEFT JOIN chipper.{{ var('core_public') }}.users AS u_ur
    ON u_ur.id = ur.user_referred_by
