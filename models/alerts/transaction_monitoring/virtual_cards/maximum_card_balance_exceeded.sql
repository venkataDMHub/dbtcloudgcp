{% set primary_currency = ("ZAR", "NGN", "UGX") %}

{% set currency_limits = {
    "ZAR": 50000,
    "NGN": 5000000,
    "UGX": 20000001,
} %}

with
    transactions as (
        select
            issued_card_transactions.user_id,
            issued_card_transactions.created_at,
            issued_card_transactions.amount,
            issued_card_transactions.currency,
            issued_card_transactions.transfer_id,
            case
                when entry_type = 'CREDIT' then amount else amount * -1
            end as issued_card_transactions_amount,
            expanded_users.primary_currency
        from
            chipper.{{ var("core_public") }}.issued_card_transactions
            as issued_card_transactions
        left join
            {{ ref("expanded_users") }}
            on issued_card_transactions.user_id = expanded_users.user_id
        where
            status in ('SETTLED', 'COMPLETED')
            and primary_currency in {{ primary_currency }}
        order by issued_card_transactions.user_id, issued_card_transactions.created_at
    ),
    balance_calculation as (
        select
            user_id,
            created_at as triggered_at,
            array_construct(transfer_id) as list_of_txns,
            primary_currency,
            currency,
            sum(issued_card_transactions_amount) over (
                partition by user_id, currency order by created_at
            ) as running_balance
        from transactions
    )
select *
from balance_calculation
where
    {% for currency, amount_limit in currency_limits.items() %}
    (
        currency = '{{ currency }}'
        and primary_currency = '{{ currency }}'
        and running_balance >= {{ amount_limit }}
    )
    {{ "or" if not loop.last }}
    {% endfor %}
    or (primary_currency = 'NGN' and currency = 'USD' and running_balance >= 5001)
qualify row_number() over (partition by user_id order by triggered_at) = 1
