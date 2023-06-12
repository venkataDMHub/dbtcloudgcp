{{ config(materialized='ephemeral') }}

select
    users.id as user_id,
    case
        when
            referral_acquired_users.user_id is not null then referral_acquired_users.acquisition_source
        when branch_install.user_id is not null then branch_install.acquisition_source
        else 'Unknown'
    end as acquisition_source
from chipper.{{ var("core_public") }}.users as users
left join
    {{ ref('referral_acquired_users') }} as referral_acquired_users on
        users.id = referral_acquired_users.user_id
left join
    {{ ref('first_branch_install_corrected') }} as branch_install on
        users.id = branch_install.user_id
