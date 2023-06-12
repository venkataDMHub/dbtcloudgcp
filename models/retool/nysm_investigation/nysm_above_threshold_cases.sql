{{ config(materialized="ephemeral") }}
{% set nysm_threshold = 0.92 %}
{% set nysm_manual_review_lower_bound = 0.88 %}
with
    expanded_nysm_results as (
        select
            nysm_results.user_id,
            nysm_results.created_at,
            v.value:score as highest_match_score,
            nysm_results.highest_match_bucket,
            v.value:user_id::varchar as highest_match_id,
            nysm_results.num_high_matches,
            nysm_results.num_medium_matches
        from
            {{ var("compliance_public") }}.nysm_results as nysm_results,
            lateral flatten(input => nysm_results.topmatches) v

    ),
    expanded_nysm_info as (
        select
            expanded_nysm_results.user_id as src_user_id,
            expanded_nysm_results.created_at,
            expanded_nysm_results.highest_match_score,
            expanded_nysm_results.highest_match_bucket,
            expanded_nysm_results.highest_match_id,
            expanded_nysm_results.num_high_matches,
            expanded_nysm_results.num_medium_matches,
            users.primary_currency,
            user_tiers.tier
        from expanded_nysm_results
        left join
            {{ var("core_public") }}.users on users.id = expanded_nysm_results.user_id
        left join
            {{ var("compliance_public") }}.user_tiers
            on user_tiers.user_id = expanded_nysm_results.user_id
        where expanded_nysm_results.user_id != expanded_nysm_results.highest_match_id
        qualify
            row_number() over (
                partition by
                    expanded_nysm_results.user_id,
                    expanded_nysm_results.highest_match_id
                order by expanded_nysm_results.created_at desc
            )
            = 1
    )

-- actual cases that exceeded the threshold for manual review
(
    select * 
    from expanded_nysm_info 
    where highest_match_score > {{ nysm_threshold }}
)
union
-- cases that did not exceed the threshold with a close score
-- randomly sample 10 cases everyday for same manual review process for future model improvement
(
    select *
    from expanded_nysm_info
    where
        highest_match_score >= {{ nysm_manual_review_lower_bound }}
        and highest_match_score <= {{ nysm_threshold }}
    qualify row_number() over (partition by date(created_at) order by random()) <= 10
)
