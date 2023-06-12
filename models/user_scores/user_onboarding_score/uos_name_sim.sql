WITH names AS (
    SELECT
        first_name,
        last_name
    FROM
        chipper.{{var('core_public')}}.users
    WHERE
        TIMESTAMPDIFF(HOUR, created_at, CURRENT_TIMESTAMP) <= 24
),

agg_names AS (
    (
        SELECT first_name AS name
        FROM
            names
    )
    UNION ALL
    (
        SELECT last_name AS name
        FROM
            names
    )
)

SELECT
    name,
    COUNT(*) AS freq_name
FROM
    agg_names
GROUP BY
    name
ORDER BY
    freq_name DESC
