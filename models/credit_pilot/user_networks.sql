{{ config(materialized = 'ephemeral') }}

with users_referred as (

    select
        referrer_id as user_id,
        count(distinct invited_user_id) as number_users_referred
    from {{ ref('expanded_referrals') }}
    group by 1
    order by number_users_referred desc

),

p2p_network as (

    select
        main_party_user_id as user_id,
        count(distinct counter_party_user_id) as total_p2p_network
    from {{ ref('expanded_ledgers') }}
    where main_party_user_id != counter_party_user_id
        and transfer_type in (
            'PAYMENTS_P2P_SETTLED', 'PAYMENT_INVITATIONS_SETTLED', 'REQUESTS_SETTLED'
        )
        and is_original_transfer_reversed is not null
    group by 1

),

final as (
    select
        number_users_referred,
        total_p2p_network,
        coalesce(ref.user_id, p2p.user_id) as user_id
    from users_referred as ref
    full outer join p2p_network as p2p
        on ref.user_id = p2p.user_id
)

select * from final
