{{ config( schema='intermediate') }}

with all_invalid_user_ids_with_reason as (
    
    select
        user_id,
        reason
    from {{ ref('invalid_referrals') }}
    union
    select
        user_id,
        reason
    from {{ ref('ngn_kes_2019_fake_accounts') }}
    union
    select
        user_id,
        reason
    from {{ ref('zar_2020_fake_accounts') }}
    union
    select
        user_id,
        reason
    from {{ ref('sardine_false_negatives') }}
    union
    select
        user_id,
        reason
    from {{ ref('non_active_sept_oct_users') }}

)

select *
from all_invalid_user_ids_with_reason
