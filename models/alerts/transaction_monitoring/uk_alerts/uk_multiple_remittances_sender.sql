{% set valid_transfer_types = ('PAYMENTS_P2P_SETTLED', 'REQUESTS_SETTLED', 'PAYMENT_INVITATIONS_SETTLED') %}

{% set multiple_remittances_count_threshold = 10 %}

select
    main_party_user_id as user_id,
    count(distinct counter_party_user_id) as num_of_receivers,
    max(hlo_created_at) as triggered_at,
    array_agg(transfer_id) as list_of_txns,
    datediff(hour, max(hlo_created_at), current_timestamp) AS time_difference
from {{ref('expanded_ledgers')}} as expanded_ledgers
inner join CHIPPER.{{var("core_public")}}.USERS on expanded_ledgers.main_party_user_id = users.id
where
    transfer_type in {{valid_transfer_types}}
    and ledger_amount < 0
    and hlo_created_at between current_timestamp + interval '-6 hour' and current_timestamp
    and not is_internal
    and primary_currency = 'GBP'
group by 1
having num_of_receivers >= {{multiple_remittances_count_threshold}}
