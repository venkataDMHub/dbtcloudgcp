{{ config(materialized='ephemeral') }}

WITH sardine_fingerprints AS (
    SELECT
        user_id,
        device_id,
        provider,
        timestamp,
        TRIM(provider_response :fingerprint) AS fingerprint,
        ROW_NUMBER() OVER(
            PARTITION BY user_id
            ORDER BY timestamp DESC
        ) AS n_id
    FROM chipper.{{ var("core_public") }}.device_fingerprints
    WHERE provider = 'SARDINE'
        AND fingerprint IS NOT NULL
),

latest_sardine_fingerprints AS (
    SELECT *
    FROM sardine_fingerprints
    WHERE n_id = 1
),

referrals AS (
    SELECT
        id AS referral_id,
        referrer_id,
        invited_user_id,
        status AS referral_status,
        created_at AS referral_created_at,
        processed_device_id,
        device_id,
        fingerprint,
        provider,
        timestamp,
        ROW_NUMBER() OVER(
            PARTITION BY fingerprint, referrer_id
            ORDER BY created_at ASC
        ) AS processing_rank
    FROM chipper.{{ var("core_public") }}.referrals
    INNER JOIN
        latest_sardine_fingerprints ON
            referrals.invited_user_id = latest_sardine_fingerprints.user_id
    WHERE fingerprint IS NOT NULL
)

SELECT
    invited_user_id AS user_id,
    'SARDINE_FALSE_NEGATIVE' AS reason
FROM referrals
WHERE processing_rank > 3
    AND referral_status = 'SETTLED'
ORDER BY referral_id
