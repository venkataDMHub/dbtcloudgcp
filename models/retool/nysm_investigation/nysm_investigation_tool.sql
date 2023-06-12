{% set case_id_column_list = ['nysm_above_threshold_cases.src_user_id', 'nysm_above_threshold_cases.highest_match_id'] %}

with
    user_latest_selfie as (
        select user_id, face_url, updated_at
        from chipper.{{ var("compliance_public") }}.liveness_checks
        qualify row_number() over (partition by user_id order by updated_at desc) = 1
    )
select
    -- column is added in order to set up reverse ETL on hightouch that requires a column to be unique to be used as a primary key
    {{ dbt_utils.surrogate_key(case_id_column_list)}} as case_id,
    nysm_above_threshold_cases.created_at,
    nysm_above_threshold_cases.src_user_id,
    nysm_above_threshold_cases.highest_match_id,
    nysm_above_threshold_cases.primary_currency,
    nysm_above_threshold_cases.highest_match_bucket,
    nysm_above_threshold_cases.highest_match_score,
    caught_user_selfie.face_url as src_face_url,
    nysm_above_threshold_cases.tier,
    match_user_selfie.face_url as matched_face_url,
    nysm_above_threshold_cases.num_high_matches,
    nysm_above_threshold_cases.num_medium_matches,
    ifnull(caught_user_balance.total_balance_usd, 0) as total_user_balance_usd,
    ifnull(match_user_balance.total_balance_usd, 0) as total_matched_user_balance_usd
from {{ ref("nysm_above_threshold_cases") }} as nysm_above_threshold_cases
left join
    user_latest_selfie as caught_user_selfie
    on caught_user_selfie.user_id = nysm_above_threshold_cases.src_user_id
left join
    user_latest_selfie as match_user_selfie
    on match_user_selfie.user_id = nysm_above_threshold_cases.highest_match_id
left join
    {{ ref("user_with_balances") }} as caught_user_balance
    on nysm_above_threshold_cases.src_user_id = caught_user_balance.user_id
left join
    {{ ref("user_with_balances") }} as match_user_balance
    on nysm_above_threshold_cases.highest_match_id = match_user_balance.user_id
where match_user_selfie.updated_at < nysm_above_threshold_cases.created_at
