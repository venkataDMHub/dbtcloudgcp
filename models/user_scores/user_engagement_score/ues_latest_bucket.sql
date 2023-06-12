WITH latest_update AS (
    SELECT
        user_id,
        user_engagement_bucket,
        user_engagement_score,
        updated_at,
        ROW_NUMBER() OVER (
            PARTITION BY user_id ORDER BY updated_at DESC
        ) AS rank_by_time
    FROM chipper.utils.user_engagement_score
)


SELECT
    user_id,
    user_engagement_bucket AS last_bucket,
    user_engagement_score AS last_score,
    updated_at
FROM latest_update
WHERE rank_by_time = 1
