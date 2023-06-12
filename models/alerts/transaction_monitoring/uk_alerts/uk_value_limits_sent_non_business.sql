{% set valid_transfer_types= ('PAYMENTS_P2P_SETTLED', 'REQUESTS_SETTLED', 'PAYMENT_INVITATIONS_SETTLED') %}

{% set value_send_limit={
        'GPB': 6000.00
    }
%}
with gbp_cross_border_txns as (

    select
        expanded_ledgers.main_party_user_id,
        expanded_ledgers.transfer_id,
        expanded_ledgers.hlo_created_at,
        expanded_ledgers.ledger_amount,
        expanded_ledgers.ledger_currency
    from
        {{ref('expanded_ledgers')}} as expanded_ledgers
    inner join {{ref('expanded_users')}} as expanded_users
        on expanded_ledgers.main_party_user_id = expanded_users.user_id
    where
        transfer_type in {{valid_transfer_types}}
        and ledger_amount < 0
        and is_business = FALSE
        and primary_currency = 'GBP'
        and case when ledger_currency = 'GHS' then corridor not like '%CROSS%'
            else corridor like '%CROSS%'
        end
    order by main_party_user_id, hlo_created_at

),
gbp_users_running_balance as (
    select
        main_party_user_id,
        transfer_id,
        hlo_created_at,
        ledger_amount,
        ledger_currency,
        sum(
            abs(ledger_amount)
        ) over (
            partition by main_party_user_id, ledger_currency order by hlo_created_at
        ) as running_balance,
        running_balance - abs(ledger_amount) as previous_running_balance
    from gbp_cross_border_txns

    where
        cast(
            hlo_created_at as date
        ) between dateadd('day', -7, current_date()) and current_date()
    order by main_party_user_id, hlo_created_at
),

list_of_txns as (
    select
        main_party_user_id as user_id,
        ledger_currency,
        array_agg(transfer_id) as list_of_txns,
        max(running_balance) as running_balance,
        max(hlo_created_at) as triggered_at

    from
        gbp_users_running_balance

    where
        {% for currency, value_sent_limit in value_send_limit.items() %}
        (ledger_currency = '{{ currency }}' and previous_running_balance <= {{ value_sent_limit }})
    {{ "or" if not loop.last }}
    {% endfor %}

    group by
        main_party_user_id,
        ledger_currency


)



select
    user_id,
    list_of_txns,
    ledger_currency,
    running_balance,
    triggered_at

from list_of_txns
where
    {% for currency, value_sent_limit in value_send_limit.items() %}
    (ledger_currency = '{{ currency }}' and running_balance > {{value_sent_limit}})
    {{ "or" if not loop.last }}
    {% endfor %}
