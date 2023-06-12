{{ config(materialized='ephemeral') }}

with zar_users as (
    select id as user_id
    from chipper.{{ var("core_public") }}.users
    where cast(created_at as date) between '2020-08-01' and '2020-08-31'
               and primary_currency in ('ZAR')
),

active_account_flag as (
    select user_id

    from
        chipper.{{var("compliance_public")}}.account_flags
    where

        date_unflagged is null
    group by
        user_id

)

select
    user_id,
    'ZAR_2020_FAKE_ACCOUNTS' as reason
from zar_users
where
    exists (
        select active_account_flag.user_id
        from active_account_flag where active_account_flag.user_id = zar_users.user_id
    )
