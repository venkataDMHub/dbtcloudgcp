{{ config(materialized = 'ephemeral') }}

with ngn_kes_users as (
    select
        id as user_id,
        created_at
    from chipper.{{ var("core_public") }}.users
    where created_at between '2019-09-01' and '2019-11-30'
        and primary_currency in ('NGN', 'KES')
),

any_transfer_after_4_weeks_of_user_creation as (
    select n.user_id
    from ngn_kes_users as n
    where
        exists (
            select l.main_party_user_id
            from {{ ref('expanded_ledgers') }} as l
            where n.user_id = l.main_party_user_id
                  and l.hlo_updated_at > dateadd(day, 28, n.created_at)
                  and l.hlo_status in ('SETTLED', 'COMPLETED')
                  and l.is_original_transfer_reversed = false
        )
)

select
    n.user_id,
    'NGN_KES_2019_FAKE_ACCOUNTS' as reason
from ngn_kes_users as n
where
    n.user_id not in (select user_id from any_transfer_after_4_weeks_of_user_creation)
