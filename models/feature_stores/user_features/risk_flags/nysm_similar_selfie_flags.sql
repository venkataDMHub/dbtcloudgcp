{{ config(materialized="view") }}

{% set highest_match_score_threshold = {"GBP": 0.92,
                                        "GHS": 0.92,
                                        "NGN": 0.92,
                                        "RWF": 0.92,
                                        "UGX": 0.92,
                                        "USD": 0.92,
                                        "ZAR": 0.92} %}

with
nysm_latest as (
    select
        user_id,
        highest_match_score,
        created_at
    from chipper.{{ var("compliance_public") }}.nysm_results
    qualify row_number() over (partition by user_id order by created_at desc) = 1
),
nysm_whitelist as (
    select
        user_id,
        true as is_whitelisted_nysm,
        max(date_flagged) as whitelisted_nysm_added_at
    from chipper.{{ var("compliance_public") }}.account_flags
    where flag = 'WHITELISTED_NYSM' and date_unflagged is null
    group by user_id, is_whitelisted_nysm
),

check_nysm as (
    select
        nysm_latest.user_id as user_id,
        nysm_latest.highest_match_score,
        nysm_latest.created_at as latest_match_score_created_at,
        nysm_whitelist.whitelisted_nysm_added_at,
        coalesce(nysm_whitelist.is_whitelisted_nysm, false) as is_whitelisted_nysm
    from nysm_latest
    left join nysm_whitelist on nysm_latest.user_id = nysm_whitelist.user_id

)
select distinct
    check_nysm.user_id,
    'NYSM' as risk_type,
    'FLAGGED_BY_SIMILAR_SELFIE' as risk_reason
from check_nysm
left join
    chipper.{{ var("core_public") }}.users as users on check_nysm.user_id = users.id
where
    check_nysm.is_whitelisted_nysm = false
    and {% for currency, match_score in highest_match_score_threshold.items() %}
    (primary_currency = '{{ currency }}' and highest_match_score >= {{ match_score }})
    {{ "or" if not loop.last }}
    {% endfor %}
