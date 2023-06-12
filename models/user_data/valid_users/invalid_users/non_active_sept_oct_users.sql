{{ config(materialized = 'ephemeral') }}

with sept_oct_transactions_ngn as (
    select distinct
        id as user_id,
        u.created_at,
        hlo_updated_at
    from dbt_transformations.expanded_ledgers as l
    left join chipper.{{ var("core_public") }}.users as u
        on l.main_party_user_id = u.id
    where created_at between '2021-09-01' and '2021-10-21'
        and datediff('day', created_at, hlo_updated_at) > 28
        and is_original_transfer_reversed = false
        and hlo_status in ('SETTLED', 'COMPLETED')
        and u.primary_currency = 'NGN'

),

sept_oct_non_active_ngn as (
    select distinct
        id as user_id,
        created_at
    from chipper.{{ var("core_public") }}.users
    where created_at between '2021-09-01' and '2021-10-21'
        and primary_currency = 'NGN'
        and user_id not in (select distinct user_id from sept_oct_transactions_ngn)
)

select
    s.user_id,
    'NGN_2021_FAKE_ACCOUNTS' as reason
from sept_oct_non_active_ngn as s
