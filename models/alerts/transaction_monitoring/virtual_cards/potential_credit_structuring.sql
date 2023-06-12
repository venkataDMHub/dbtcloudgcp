{% set primary_currency = ("USD", "GHS", "ZAR", "NGN", "UGX") %}

{% set total_amount_limits = {
    "USD": 12750,
    "GHS": 75000,
    "ZAR": 74985,
    "NGN": 15000000,
    "UGX": 52000000,
} %}

{% set transaction_amount_limits = {
    "USD": 212.50,
    "GHS": 1250,
    "ZAR": 1250,
    "NGN": 250000,
    "UGX": 875000,
} %}

with
    dates as (
        select distinct
            cast(created_at as date) - interval '30' as start_date,
            cast(created_at as date) as end_date
        from chipper.{{ var("core_public") }}.issued_card_transactions
        order by 1
    ),

    transactions as (
        select
            issued_card_transactions.user_id,
            cast(issued_card_transactions.created_at as date) as triggered_at,
            year(issued_card_transactions.created_at) as year,
            issued_card_transactions.transfer_id,
            currency,
            primary_currency,
            abs(amount) as ledger_amount
        from
            chipper.{{ var("core_public") }}.issued_card_transactions
            as issued_card_transactions
        left join
            {{ ref("expanded_users") }} as expanded_users
            on issued_card_transactions.user_id = expanded_users.user_id
        where
            status in ('SETTLED', 'COMPLETED')
            and primary_currency in {{ primary_currency }}
            and entry_type like '%CREDIT%'
    ),
    summarized_transactions as (
        select
            user_id,
            max(triggered_at) as triggered_at,
            sum(ledger_amount) as total_amount_local,
            array_agg(transfer_id) as list_of_txns,
            primary_currency,
            currency
        from dates
        left join
            transactions
            on cast(transactions.triggered_at as date)
            between dates.start_date and dates.end_date
        where
            {% for currency, amount_limit in transaction_amount_limits.items() %}
            (
                currency = '{{currency}}'
                and primary_currency = '{{ currency }}'
                and (ledger_amount between 1 and {{ amount_limit }})
            )
            {{ "or" if not loop.last }}
            {% endfor %}
            or (
                primary_currency = 'NGN'
                and currency = 'USD'
                and (ledger_amount between 1 and 250)
            )

        group by user_id, primary_currency, currency

    )

select *
from summarized_transactions
where
    {% for currency, amount_limit in total_amount_limits.items() %}
    (
        currency = '{{currency}}'
        and primary_currency = '{{ currency }}'
        and total_amount_local = {{ amount_limit }}
    )
    {{ "or" if not loop.last }}
    {% endfor %}
    or (primary_currency = 'NGN' and currency = 'USD' and total_amount_local >= 15015)
