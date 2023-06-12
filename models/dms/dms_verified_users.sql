WITH hardcoded_users AS (
    SELECT
        *
    FROM
        {{ ref('hardcoded_failed_user_verification') }}
),
verified_users AS (
    SELECT
        user_tiers.user_id,
        user_tiers.tier AS STATUS,
        user_tiers.updated_at AS created_at,
        users.primary_currency
    FROM
        {{var("compliance_public")}}.user_tiers
        JOIN {{var("core_public")}}.users ON user_tiers.user_id = users.id
    WHERE
        user_tiers.updated_at BETWEEN DATEADD(
            'HOUR',
            -1,
            DATE_TRUNC('HOUR', CURRENT_TIMESTAMP())
        )
        AND DATE_TRUNC('HOUR', CURRENT_TIMESTAMP())
        AND tier = 'VERIFIED'
)
SELECT
    *
FROM
    verified_users
UNION
SELECT
    *
FROM
    hardcoded_users
