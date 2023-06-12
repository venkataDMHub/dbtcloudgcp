with latest_number as (
    select
        user_id,
        identifier as phone_number
    from chipper.{{ var("core_public") }}.contacts
    where type = 'phone'
    qualify row_number() over (partition by user_id order by created_at desc) = 1
),

latest_email as (
    select
        user_id,
        identifier as email_address
    from chipper.{{ var("core_public") }}.contacts
    where type = 'email'
    qualify row_number() over (partition by user_id order by created_at desc) = 1
)

select
    users.id as user_id,
    users.avatar,
    users.first_name as display_first_name,
    users.last_name as display_last_name,
    user_info.first_name as legal_first_name,
    user_info.last_name as legal_last_name,
    users.is_admin,
    users.primary_currency,
    user_acquisition_source.acquisition_source,
    user_tiers.tier as kyc_tier,
    users.pin,
    users.created_at,
    users.updated_at,
    users.tag,
    latest_number.phone_number,
    latest_email.email_address,
    users.is_internal,
    users.is_deleted,
    users.is_business,
    invalid_user_reasons,
    flag,
    user_flags.all_active_flags,
    user_flags.num_flags,
    case when invalid_user_ids.user_id is not null then FALSE else TRUE end as is_valid_user,
    case
        when user_ids_with_account_blocking_flag.user_id is null then FALSE else TRUE
    end as is_blocked_by_flag,
    case when users.id in ({{risk_flag_users()}}) then TRUE else FALSE end as has_risk_flag
from chipper.{{ var("core_public") }}.users as users
left join
    chipper.{{ var("compliance_public") }}.user_info as user_info on users.id = user_info.user_id
left join
    chipper.{{ var("compliance_public") }}.user_tiers on users.id = user_tiers.user_id
left join
    {{ ref('user_acquisition_source') }} as user_acquisition_source on
        users.id = user_acquisition_source.user_id
left join
    {{ ref('invalid_user_ids') }} as invalid_user_ids on users.id = invalid_user_ids.user_id
left join
    {{ ref('user_ids_with_account_blocking_flag') }} as user_ids_with_account_blocking_flag on
        users.id = user_ids_with_account_blocking_flag.user_id
left join
    latest_number on users.id = latest_number.user_id
left join
    latest_email on users.id = latest_email.user_id
left join 
    {{ ref('user_all_flags') }} as user_flags
        on users.id = user_flags.user_id
