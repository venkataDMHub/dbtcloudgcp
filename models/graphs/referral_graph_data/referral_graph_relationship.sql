SELECT
    REFERRER_ID,
    INVITED_USER_ID,
    CREATED_AT AS REFERRAL_CREATED_AT,
    STATUS
FROM CHIPPER.{{ var("core_public") }}.REFERRALS
