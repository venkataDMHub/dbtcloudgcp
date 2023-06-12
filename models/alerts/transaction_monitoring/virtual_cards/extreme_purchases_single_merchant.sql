{% set primary_currency = ("USD", "GHS", "ZAR", "NGN", "UGX") %}

{% set currency_limits = {
    "USD": 425,
    "GHS": 2500,
    "ZAR": 2500,
    "NGN": 500000,
    "UGX": 17500000,
} %}


{% set merchant_transaction_limit = 20 %}

with
    transactions as (
        select
            issued_cards_txns.user_id,
            cast(issued_cards_txns.created_at as date) as triggered_at,
            day_of_week,
            week_of_year,
            year(dim_dates.date) as year,
            issued_cards_txns.currency,
            primary_currency,
            abs(amount) as ledger_amount,
            issued_cards_txns.provider_details:"MerchantID" as merchant_id,
            array_construct(transfer_id) as transfer_ids
        from
            chipper.{{ var("core_public") }}.issued_card_transactions
            as issued_cards_txns
        left join
            {{ ref("expanded_users") }}
            on issued_cards_txns.user_id = expanded_users.user_id
        left join
            {{ ref("dim_dates") }} as dim_dates
            on cast(issued_cards_txns.created_at as date) = dim_dates.date
        where
            status in ('SETTLED', 'COMPLETED')
            and issued_cards_txns.type = 'TRANSACTION'
            and issued_cards_txns.entry_type = 'DEBIT'
            and primary_currency in {{ primary_currency }}
            and issued_cards_txns.provider_details:"MerchantID" is not null
    ),
    weekly_grouping as (
        select
            user_id,
            week_of_year,
            year,
            max(triggered_at) as triggered_at,
            sum(ledger_amount) as sum_amount,
            currency,
            primary_currency,
            merchant_id,
            count(*) as count_merchant_txns,
            array_agg(transfer_ids) as list_of_txns
        from transactions
        group by user_id, week_of_year, currency, primary_currency, merchant_id, year 
        having
            count_merchant_txns >= {{ merchant_transaction_limit }}
            and (
                {% for currency, amount_limit in currency_limits.items() %}
                (
                    currency = '{{ currency }}'
                    and primary_currency = '{{ currency }}'
                    and sum_amount >= {{ amount_limit }}
                )
                {{ "or" if not loop.last }}
                {% endfor %}
                or (primary_currency = 'NGN' and currency = 'USD' and sum_amount >= 500)
            )


    )
select *
from weekly_grouping
qualify row_number() over (partition by user_id, week_of_year order by triggered_at) = 1
