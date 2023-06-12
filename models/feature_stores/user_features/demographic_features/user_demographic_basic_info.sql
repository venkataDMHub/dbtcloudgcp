{{
    config(
        materialized='ephemeral'
    )
}}

with user_demographics as (
    select
        user_id,
        nationality,
        city_of_birth,
        dob,
        purpose_of_account,
        (case when gender = 'Male' then 'M'
                when gender = 'N' then null
                when gender = 'N/A' then null
                else gender
            end) as gender,
        datediff(year, dob, current_date()) as age
    from chipper.{{ var("compliance_public") }}.user_info
),

user_accepted_selfies as (
    select
        user_id,
        -- create a list of all accepted selfie urls 
        array_agg(face_url) within group (order by created_at asc) as face_urls
    from chipper.{{ var("compliance_public") }}.liveness_checks
    group by user_id
)

select
    e.user_id,
    e.display_first_name,
    e.display_last_name,
    e.legal_first_name,
    e.legal_last_name,
    d.nationality,
    d.city_of_birth,
    d.dob,
    d.age as user_age,
    d.gender,
    e.tag,
    e.avatar,
    e.primary_currency,
    e.acquisition_source,
    e.created_at as acquisition_date,
    e.kyc_tier,
    d.purpose_of_account,
    e.is_deleted,
    e.is_internal,
    e.is_admin,
    e.is_business,
    e.is_valid_user,
    e.invalid_user_reasons,
    e.is_blocked_by_flag,
    e.has_risk_flag,
    e.num_flags,
    e.phone_number,
    e.email_address,
    s.face_urls,
    datediff(day, e.created_at, current_date()) as account_age,
    array_to_string(e.all_active_flags, ', ') as all_active_flags
from {{ ref("expanded_users") }} as e
left join user_accepted_selfies as s on e.user_id = s.user_id
left join user_demographics as d on e.user_id = d.user_id
