{% set primary_currency = ("USD", "GHS", "ZAR", "NGN", "UGX") %}
with
    transactions as (
        select
            issued_card_transactions.user_id,
            issued_card_transactions.created_at as triggered_at,
            day_of_year,
            week_of_year,
            year(issued_card_transactions.created_at) as year_created,
            transfer_id,
            currency,
            primary_currency,
            case
                when entry_type = 'CREDIT' then amount else amount * -1
            end as ledger_amount
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
            status in ('SETTLED', 'COMPLETED')
            and primary_currency in {{ primary_currency }}
    ),
    balance_calculations as (
        select
            user_id,
            triggered_at,
            day_of_year,
            year_created,
            currency,
            primary_currency,
            array_agg(transfer_id) as list_of_txns,
            sum(
                case when ledger_amount > 0 then abs(ledger_amount) else 0 end
            ) as sum_transactions_funding_local,
            sum(
                case when ledger_amount < 0 then abs(ledger_amount) else 0 end
            ) as sum_transactions_withdrawal_local
        from transactions
        group by
            user_id, triggered_at, day_of_year, currency, primary_currency, year_created
    )
select *
from balance_calculations
where
    (
        currency = 'USD'
        and sum_transactions_funding_local >= 765
        and sum_transactions_withdrawal_local >= 688
    )
    or (
        currency = 'GHS'
        and sum_transactions_funding_local >= 4500
        and sum_transactions_withdrawal_local >= 4050
    )
    or (
        currency = 'ZAR'
        and sum_transactions_funding_local >= 4499
        and sum_transactions_withdrawal_local >= 4049
    )
    or (
        primary_currency = 'NGN'
        and currency = 'NGN'
        and sum_transactions_funding_local >= 900000
        and sum_transactions_withdrawal_local >= 810000
    )
    or (
        primary_currency = 'NGN'
        and currency = 'USD'
        and sum_transactions_funding_local >= 900
        and sum_transactions_withdrawal_local >= 810

    )
    or (
        currency = 'UGX'
        and sum_transactions_funding_local >= 3150000
        and sum_transactions_withdrawal_local >= 2835000
    )
