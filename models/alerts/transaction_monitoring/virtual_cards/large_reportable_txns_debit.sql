{% set primary_currency = ("USD", "GHS", "ZAR", "NGN", "UGX") %}

{% set currency_limits = {
    "USD": 851,
    "GHS": 5000,
    "ZAR": 5001,
    "NGN": 2000001,
    "UGX": 3500001,
} %}

with
    balance as (
        select
            issued_card_transactions.user_id,
            cast(issued_card_transactions.created_at as date) as triggered_at,
            sum(abs(amount)) as funding_amount,
            array_agg(transfer_id) as list_of_txns,
            issued_card_transactions.currency,
            primary_currency
        from
            chipper.{{ var("core_public") }}.issued_card_transactions
            as issued_card_transactions
        left join
            {{ ref("expanded_users") }}
            on issued_card_transactions.user_id = expanded_users.user_id
        where
            status in ('SETTLED', 'COMPLETED')
            and primary_currency in {{ primary_currency }}
            and entry_type like '%DEBIT%'
        group by
            cast(issued_card_transactions.created_at as date),
            issued_card_transactions.user_id,
            currency,
            primary_currency

    )
select user_id, triggered_at, funding_amount, currency, list_of_txns, primary_currency
from balance
where
    {% for currency, amount_limit in currency_limits.items() %}
    (
        currency = '{{ currency }}'
        and primary_currency = '{{ currency }}'
        and funding_amount >= {{ amount_limit }}
    )
    {{ "or" if not loop.last }}
    {% endfor %}
    or (primary_currency = 'NGN' and currency = 'USD' and funding_amount >= 1001)
