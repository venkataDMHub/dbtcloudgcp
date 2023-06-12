{% set 
    alert_levels = {
        'TIER_1': ["POTENTIAL_MATCH"],
        'TIER_3': ["POTENTIAL_MATCH_ESCALATED", "FURTHER_INVESTIGATION_REQUIRED"],
        'COMPLETED': ["TRUE_MATCH_BLOCKED", "TRUE_MATCH_ALLOWED", "NO_MATCH"],
        'MLRO': ["TRUE_MATCH_RECOMMEND_ALLOW", "TRUE_MATCH_RECOMMEND_BLOCK"]
    }
%}

{{
    config(
        materialized='table'
    )
}}

WITH alert_assignments AS (
    SELECT
        alert_assignments.user_id,
        alert_assignments.admin_id,
        alert_assignments.assigned_at,
        alert_assignees.email as analyst_email,
        alert_assignees.type as analyst_level
    FROM 
        {{ var('compliance_public') }}.alert_assignments
        LEFT JOIN {{ var('compliance_public') }}.alert_assignees
            ON alert_assignments.admin_id = alert_assignees.user_id
    WHERE
        alert_assignments.type = 'WATCHLIST'
    -- get latest assignments
    QUALIFY row_number() over(partition by alert_assignments.user_id order by assigned_at desc) = 1
)
SELECT
    -- watchlist alert info
    distinct
    watchlist_matches.user_id,
    watchlist_matches.status,
    watchlist_matches.watchlist,
    watchlist_matches.match_type,
    -- user level info
    user_tiers.tier as user_tier,
    user_info.country_of_birth,
    user_info.first_name,
    user_info.last_name,
    user_info.dob as date_of_birth,
    kyc_documents.issuing_country as kyc_country,
    -- alert assignment info
    case 
        when alert_assignments.user_id is null then False
        else True 
    end as is_assigned_alert,
    -- alert level statistics
    count(watchlist_matches.id) over(partition by watchlist_matches.user_id) as num_total_alerts,
    -- get alert level mapping
    case 
    {% for alert_level, alert_statuses in alert_levels.items() %}
        {% for alert in alert_statuses %}
            when watchlist_matches.status = '{{alert}}' then '{{alert_level}}'
        {% endfor %}
    {% endfor %}
        else 'OTHER'
    end as watchlist_alert_level,
    -- alert assignment analyst info
    alert_assignments.admin_id,
    alert_assignments.assigned_at,
    alert_assignments.analyst_email,
    alert_assignments.analyst_level
FROM
    {{ var("compliance_public") }}.watchlist_matches
    LEFT JOIN alert_assignments
        ON watchlist_matches.user_id = alert_assignments.user_id
    LEFT JOIN {{ var("compliance_public") }}.user_info
        ON watchlist_matches.user_id = user_info.user_id
    LEFT JOIN {{ var("compliance_public") }}.user_tiers
        ON watchlist_matches.user_id = user_tiers.user_id
    LEFT JOIN {{ var("compliance_public" ) }}.kyc_documents
        ON watchlist_matches.user_id = kyc_documents.owner_id
WHERE
    kyc_documents.status = 'ACCEPTED'

