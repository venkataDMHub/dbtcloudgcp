WITH REFERRAL_USERS AS (
    SELECT REFERRER_ID AS USER_ID FROM {{ ref('referral_graph_relationship') }}
    UNION DISTINCT
    SELECT INVITED_USER_ID AS USER_ID
    FROM {{ ref('referral_graph_relationship') }}
), 
REFERRAL_PAYOUTS AS (
    SELECT MAIN_PARTY_USER_ID AS USER_ID,
        COUNT(*) AS TOTAL_LIFETIME_REFERRAL_PAYOUT_TRANSFERS,
        SUM(LEDGER_AMOUNT_IN_USD) AS TOTAL_LIFETIME_REFERRAL_PAYOUT_USD
    FROM {{ ref('expanded_ledgers') }}
    WHERE TRANSFER_TYPE in ('PAYMENTS_REFERRAL_BONUS_SETTLED', 'REFERRAL_BONUS_SETTLED')
    GROUP BY USER_ID
),
SIMILAR_SELFIE AS (
  SELECT DISTINCT decision_response_body:PartnerParams:user_id AS USER_ID,
         True AS HAS_SIMILAR_SELFIE
    FROM CHIPPER.{{var("compliance_public")}}.SMILE_ID_JOBS
    WHERE decision_response_body:Actions:SimilarFacesFound = 'Found'
)

SELECT
    EXPANDED_USERS.USER_ID,
    EXPANDED_USERS.PRIMARY_CURRENCY,
    EXPANDED_USERS.CREATED_AT AS USER_CREATED_AT,
    EXPANDED_USERS.AVATAR,
    EXPANDED_USERS.DISPLAY_FIRST_NAME,
    EXPANDED_USERS.DISPLAY_LAST_NAME,
    LATEST_SELFIE.FACE_URL AS USER_SELFIE_URL,
    LATEST_UES.LAST_BUCKET AS USER_ENGAGEMENT_BUCKET,
    LATEST_UES.LAST_SCORE AS USER_ENGAGEMENT_SCORE,
    LATEST_UOS.UOS_SCORE AS USER_ONBOARDING_SCORE,
    REFERRAL_PAYOUTS.TOTAL_LIFETIME_REFERRAL_PAYOUT_USD,
    REFERRAL_PAYOUTS.TOTAL_LIFETIME_REFERRAL_PAYOUT_TRANSFERS,
    LPV.SUB_TOTAL_LPV_USD, 
    LPV.SUB_TOTAL_LIFETIME_TRANSFERS,
    LPV.TOTAL_CHIPPER_REWARDS_PAYOUT_USD,
    LPV.TOTAL_CHIPPER_REWARDS_PAYOUT_TRANSFERS,
    LPV.TOTAL_LIFETIME_TRANSFERS,
    LPV.TOTAL_LPV_USD,
    (CASE WHEN SIMILAR_SELFIE.HAS_SIMILAR_SELFIE THEN TRUE ELSE FALSE END) AS HAS_SIMILAR_SELFIE
FROM
    {{ ref('expanded_users') }} AS EXPANDED_USERS
INNER JOIN REFERRAL_USERS
    ON EXPANDED_USERS.USER_ID = REFERRAL_USERS.USER_ID
LEFT JOIN {{ ref('ues_latest_bucket') }} AS LATEST_UES
    ON LATEST_UES.USER_ID = EXPANDED_USERS.USER_ID
-- Using latest UOS scores due to a known issue with duplicates
LEFT JOIN {{ ref('user_uos') }} AS LATEST_UOS
    ON LATEST_UOS.USER_ID = EXPANDED_USERS.USER_ID
LEFT JOIN REFERRAL_PAYOUTS 
    ON EXPANDED_USERS.USER_ID = REFERRAL_PAYOUTS.USER_ID
LEFT JOIN {{ ref('user_best_selfie') }} AS LATEST_SELFIE
    ON EXPANDED_USERS.USER_ID = LATEST_SELFIE.USER_ID
LEFT JOIN {{ ref('user_lifetime_processed_volume') }} AS LPV
    ON EXPANDED_USERS.USER_ID = LPV.USER_ID
LEFT JOIN SIMILAR_SELFIE
    ON EXPANDED_USERS.USER_ID = SIMILAR_SELFIE.USER_ID
WHERE EXPANDED_USERS.USER_ID not in ({{internal_users()}})
