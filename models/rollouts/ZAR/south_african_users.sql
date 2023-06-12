WITH SOUTH_AFRICAN_USERS AS (
    SELECT *
    FROM CHIPPER.{{ var("core_public") }}.USERS
    WHERE PRIMARY_CURRENCY = 'ZAR'
),

SHUFFLED_SA_USERS AS (
    SELECT
        *,
        -- Use randomly-ordered row numbers to assign percentiles, so we can
        -- deterministically fetch any N% of users
        (ROW_NUMBER() OVER(ORDER BY (SELECT NULL)))
        / (SELECT COUNT(*) FROM SOUTH_AFRICAN_USERS) AS PERCENTILE
    FROM SOUTH_AFRICAN_USERS
    -- Shuffle the table by randomly sampling 100% of rows
    SAMPLE ROW (100)
)

SELECT
    ID AS USER_ID,
    PERCENTILE
FROM
    SHUFFLED_SA_USERS
