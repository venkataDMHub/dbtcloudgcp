{% set receiving_transfer_type = ('DEPOSITS_SETTLED', 'PAYMENTS_P2P_SETTLED', 'REQUESTS_SETTLED','PAYMENT_INVITATIONS_SETTLED') %}

{% set sent_transfer_type = ('AIRTIME_PURCHASES_COMPLETED',
                            'PAYMENTS_P2P_SETTLED',
                            'PAYMENT_INVITATIONS_SETTLED',
                            'REQUESTS_SETTLED',
                            'WITHDRAWALS_SETTLED') %}

{% set large_deposits_followed_by_transfer_threshold_non_business={
    'USD': 471.73
    }
%}

with inbound_transfers as (
    select 
        main_party_user_id as receiver_user_id,
        array_construct(transfer_id) as inbound_transfer_id,
        transfer_type as inbound_transfer_type,
        ledger_currency as destination_currency,
        hlo_created_at as received_time,
        ledger_amount_in_usd as received_amt
    from {{ref('expanded_ledgers')}} as expanded_ledgers
    where 
        transfer_type in {{receiving_transfer_type}}
        and ledger_amount > 0
), outbound_transfers as (
    select 
        main_party_user_id  as sender_user_id,
        array_construct(transfer_id) as outbound_transfer_id,
        transfer_type as outbound_transfer_type,
        ledger_currency as sender_currency,
        hlo_created_at as sent_time,
        ledger_amount_in_usd as sent_amt,
        counter_party_user_id as outbound_transfer_receiver_user_id
    from {{ref('expanded_ledgers')}} as expanded_ledgers
    where 
        transfer_type in {{sent_transfer_type}}
        and ledger_amount < 0
), txn_list as (
select *,
    array_cat(outbound_transfer_id,inbound_transfer_id) as list_of_txns
from outbound_transfers
left join inbound_transfers
    on outbound_transfers.sender_user_id = inbound_transfers.receiver_user_id
where 
    {% for currency, deposit_limit in large_deposits_followed_by_transfer_threshold_non_business.items() %}
    received_time < sent_time
    and sent_time between received_time and received_time + interval '1 hour' 
    and round((abs(sent_amt)/received_amt * 100), 2) between 95 and 120
    and destination_currency = '{{ currency }}' and received_amt >= {{ deposit_limit }}
    {{ "or" if not loop.last }}
    {% endfor %}
order by 
    received_time,
    sent_time 
)

select 
    txn_list.*,
    expanded_users.user_id,
    sent_time as triggered_at
from txn_list
inner join {{ref('expanded_users')}} as expanded_users
on txn_list.receiver_user_id = expanded_users.user_id
where cast(sent_time as date) >= '2021-03-01'
    and expanded_users.is_business = TRUE
    and primary_currency = 'USD'
