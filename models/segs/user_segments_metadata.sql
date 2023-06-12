WITH segment_sizes AS (
    SELECT
        segment,
        count(*) AS size
    FROM
        "CHIPPER".{{var("core_public")}}."USER_SEGMENTS"
    GROUP BY
        segment
),

segment_created_and_updated_at AS (
    SELECT
        segment,
        min(updated_at) AS created_at,
        max(updated_at) AS updated_at
    FROM
        "CHIPPER"."UTILS"."HISTORICAL_USER_SEGMENTS"
    GROUP BY
        segment
)

SELECT
    segment_sizes.segment,
    size,
    created_at,
    updated_at
FROM
    segment_sizes
JOIN
    segment_created_and_updated_at ON segment_sizes.segment = segment_created_and_updated_at.segment
