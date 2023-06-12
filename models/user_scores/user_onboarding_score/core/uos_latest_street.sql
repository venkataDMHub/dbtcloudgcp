WITH user_streets AS (
    SELECT
        user_id,
        street,
        ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY created_at DESC) AS row_num
    FROM
        chipper.{{ var('compliance_public') }}.addresses
)

SELECT
    user_id,
    street
FROM
    user_streets
WHERE
    row_num = 1
