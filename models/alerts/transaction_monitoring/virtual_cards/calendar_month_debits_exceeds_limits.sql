{% set primary_currency = ("USD", "GHS", "ZAR", "NGN", "UGX") %}

{% set currency_limits = {
    "USD": 15000,
    "GHS": 50000,
    "ZAR": 25000,
    "NGN": 5000000,
    "UGX": 16000000,
} %}

with
    transactions as (
        select
            issued_card_transactions.user_id,
            cast(issued_card_transactions.created_at as date) as triggered_at,
            month_of_year,
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
        left join
            {{ ref("dim_dates") }} as dim_dates
            on cast(issued_card_transactions.created_at as date) = dim_dates.date
        where
            issued_card_transactions.status in ('SETTLED', 'COMPLETED')
            and entry_type like '%DEBIT%'
            and primary_currency in {{ primary_currency }}
    )
select
    user_id,
    primary_currency,
    currency,
    max(triggered_at) as triggered_at,
    sum(ledger_amount) as ledger_amount_total,
    array_agg(transfer_id) as list_of_txns,
    month_of_year,
    year
from transactions
group by user_id, primary_currency, currency, month_of_year, year
having
    {% for currency, amount_limit in currency_limits.items() %}
    (currency = '{{ currency }}' and ledger_amount_total >= {{ amount_limit }})
    {{ "or" if not loop.last }}
    {% endfor %}
