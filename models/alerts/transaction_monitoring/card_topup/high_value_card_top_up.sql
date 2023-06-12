{% set transfer_type = "DEPOSITS_SETTLED" %}

{% set currency_limits = {
    "USD": 9975,
    "GBP": 6650,
} %}

{% set currency_limits_currencies = ("GBP", "USD") %}

with
    settled_deposits as (
        select
            deposits_using_cards.payment_card_card_type,
            expanded_ledgers.transfer_id as list_of_txn,
            expanded_ledgers.hlo_created_at as triggered_at,
            expanded_ledgers.ledger_amount_in_usd as amount_in_usd,
            expanded_ledgers.main_party_user_id as user_id,
            expanded_ledgers.ledger_currency as currency,
            expanded_ledgers.ledger_amount,
            week_of_year,
            year(expanded_ledgers.hlo_created_at) as year_created
        from {{ ref("deposits_using_cards") }} as deposits_using_cards
        inner join
            {{ ref("expanded_ledgers") }} as expanded_ledgers
            on deposits_using_cards.transfer_id = expanded_ledgers.transfer_id
        left join
            {{ ref("dim_dates") }} as dim_dates
            on cast(expanded_ledgers.hlo_created_at as date) = dim_dates.date
        where
            transfer_type in ('{{ transfer_type }}')
            and ledger_currency in {{ currency_limits_currencies }}
    ),
    weekly_grouping as (
        select
            user_id,
            currency,
            week_of_year,
            year_created,
            list_of_txn,
            ledger_amount,
            sum(abs(ledger_amount)) over (
                partition by user_id, currency, week_of_year, year_created
                order by triggered_at
            ) as running_amount,
            case
                when
                    {% for currency, currency_limits_thresholds in currency_limits.items() %}
                    (
                        currency = '{{ currency }}'
                        and running_amount >= {{ currency_limits_thresholds }}
                    )
                    {{ "or" if not loop.last }}
                    {% endfor %}
                then
                    rank() over (
                        partition by user_id, currency, year_created, week_of_year
                        order by triggered_at
                    )
                else 0
            end as row_num,
            triggered_at
        from settled_deposits
    ),
    alerted_at_info as (
        select
            user_id, currency, week_of_year, year_created, min(row_num) as triggered_row
        from weekly_grouping
        where row_num > 0
        group by user_id, week_of_year, year_created, currency
    )
select
    weekly_grouping.user_id,
    max(triggered_at) as triggered_at,
    array_agg(list_of_txn) as list_of_txns,
    weekly_grouping.week_of_year,
    weekly_grouping.currency,
    weekly_grouping.year_created
from weekly_grouping
join
    alerted_at_info
    on weekly_grouping.user_id = alerted_at_info.user_id
    and weekly_grouping.row_num <= alerted_at_info.triggered_row
    and weekly_grouping.week_of_year = alerted_at_info.week_of_year
    and weekly_grouping.year_created = alerted_at_info.year_created
group by
    weekly_grouping.user_id,
    weekly_grouping.week_of_year,
    weekly_grouping.year_created,
    weekly_grouping.currency
