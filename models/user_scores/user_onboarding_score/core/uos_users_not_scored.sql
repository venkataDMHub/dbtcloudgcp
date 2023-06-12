SELECT
    users.id AS user_id,
    users.first_name AS first_name,
    users.last_name,
    users.created_at AS user_created_at
FROM
    chipper.{{ var("core_public") }}.users
LEFT JOIN chipper.utils.user_onboarding_score AS uos
    ON users.id = uos.user_id
WHERE
    uos.user_id IS NULL
