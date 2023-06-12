{{ config(materialized='table', schema='looker') }}

with months as ( -- get the first day of month from beg of 2018 through july 2022

    select distinct first_day_of_month
    from {{ ref('dim_dates') }}
    where year(first_day_of_month) >= 2018
        and first_day_of_month < current_date

),

exchange_rates as ( --get the exchange rates valid to/from dates
    select
        currency,
        timestamp as valid_from,
        rate,
        lead(timestamp)
        -- this is the next time the rate changes for that currency
        over (partition by currency order by timestamp) as valid_to
    from chipper.{{ var("core_public") }}.exchange_rates

),

exchange_rate_at_bom as ( --get the exchange rate at the Beg of each month

    select
        m.first_day_of_month,
        er.currency,
        er.valid_from,
        er.valid_to,
        er.rate as bom_rate
    from exchange_rates as er
    inner join months as m -- this join gets the exchange rate at the BOM for each currency
        on m.first_day_of_month >= er.valid_from
            and m.first_day_of_month < er.valid_to

),

users as ( -- excludes all risk/fraud/invalid users 

    select distinct user_id
    from {{ ref('expanded_users') }}
    where is_internal = False
        and is_valid_user = True
        and is_admin = False
        and is_deleted = False
        and is_blocked_by_flag = False
        -- excluding any user with any risk flag 
        and user_id not in (select distinct user_id from {{ ref('risk_flags') }} )

),

bom_balances as ( -- get begining of month balances for each user and each currency 

    select
        m.first_day_of_month as beginning_of_month,
        l.main_party_user_id as user_id,
        l.ledger_currency,
        er.bom_rate,
        sum(l.ledger_amount) as bom_balance,
        -- get beg of month value in USD for each currency balance 
        bom_balance * er.bom_rate as bom_balance_in_usd
    from {{ ref('expanded_ledgers') }} as l
    inner join months as m
        on m.first_day_of_month > l.ledger_timestamp -- sums any ledger amount that occured prior to the first day of month -- using ledger_timestamp for dead_end ledgers
    inner join exchange_rate_at_bom as er
        -- gets the beg of month exchange rate for each currency 
        on m.first_day_of_month = er.first_day_of_month
            and l.ledger_currency = er.currency
    inner join users as u
        on u.user_id = l.main_party_user_id
    {{ dbt_utils.group_by(n=4) }}

),

bom_balance_usd_per_user as ( -- gets BOM balance in USD across all currencies 

    select
        user_id,
        beginning_of_month,
        -- setting negative wallet balances to zero in line with this notion doc 
        -- https://www.notion.so/Finance-Reporting-Zeroing-negative-balance-ee31526706034268aa098be452fc0c2d
        case
            when sum(bom_balance_in_usd) < 0
                then 0
            else sum(bom_balance_in_usd)
        end as total_bom_balance_in_usd
    from bom_balances
    group by 1, 2

)

select *
from bom_balance_usd_per_user
