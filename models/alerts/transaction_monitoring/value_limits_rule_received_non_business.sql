{% set valid_transfer_types= ('PAYMENTS_P2P_SETTLED', 'REQUESTS_SETTLED', 'PAYMENT_INVITATIONS_SETTLED') %}

{%
    set value_limits_received={
        'NGN': 435048.34,
        'UGX': 10221266.36,
        'TZS': 1476.31,
        'GHS': 60000,
        'RWF': 100000,
        'ZAR': 1041

    }
%}


with ghana_users_running_balance as (
    select
        main_party_user_id,
        transfer_id,
        hlo_updated_at,
        ledger_amount,
        ledger_currency,
        sum(
            abs(ledger_amount)
        ) over (partition by main_party_user_id,ledger_currency order by hlo_updated_at) as running_balance,
        running_balance - abs(ledger_amount) as previous_running_balance

    from {{ref('expanded_ledgers')}} as expanded_ledgers
    inner join {{ref('expanded_users')}} as expanded_users
        on expanded_ledgers.main_party_user_id = expanded_users.user_id
    where
        transfer_type in {{valid_transfer_types}}
        and ledger_amount > 0
        and cast(
            hlo_updated_at as date
        ) between dateadd('day', -7, current_date()) and current_date()
        and is_business = FALSE
        and case when ledger_currency in ('ZAR') then corridor in ('CROSS_BORDER_FIAT','LOCAL_FIAT')
        else corridor like '%CROSS%'
        end

    order by main_party_user_id, hlo_updated_at
),list_of_txns as(
select
    main_party_user_id as user_id,
    array_agg(transfer_id) as list_of_txns,
    ledger_currency,
    max(running_balance) as running_balance,
    max(hlo_updated_at) as triggered_at

from
    ghana_users_running_balance

where

    {% for currency, value_limits_received in value_limits_received.items() %}
    (ledger_currency = '{{ currency }}' and previous_running_balance <= {{ value_limits_received }})
    {{ "or" if not loop.last }}
    {% endfor %}

group by
    main_party_user_id,
    ledger_currency

)

select 
    * 
    from list_of_txns
where
    {% for currency, value_limits_received in value_limits_received.items() %}
    (ledger_currency = '{{ currency }}' and running_balance > {{value_limits_received}})
    {{ "or" if not loop.last }}
    {% endfor %}
