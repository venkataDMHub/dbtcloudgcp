{{
    config(
        unique_key='user_id',
        schema='intermediate'
    )
}}

with nysm_latest as (
    SELECT
        user_id,
        highest_match_score,
        created_at
    FROM
        {{ var("compliance_public") }}.nysm_results
    QUALIFY ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY highest_match_score desc) = 1
),
nysm_whitelist as (
	SELECT
		user_id,
		TRUE as is_whitelisted_nysm,
        max(date_flagged) as whitelisted_nysm_added_at
	FROM
		{{ var("compliance_public") }}.account_flags
    WHERE
        flag in ('WHITELISTED_NYSM')
        AND date_unflagged is NULL
    {{ dbt_utils.group_by(n=2) }}
)

SELECT
    nysm_latest.user_id,
    nysm_latest.highest_match_score,
    nysm_latest.created_at,
    ifnull(nysm_whitelist.is_whitelisted_nysm, False) as is_whitelisted_nysm,
    whitelisted_nysm_added_at
FROM nysm_latest 
    LEFT JOIN nysm_whitelist ON nysm_latest.user_id = nysm_whitelist.user_id
