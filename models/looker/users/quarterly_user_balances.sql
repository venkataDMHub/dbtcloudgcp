{{ config(materialized='table', schema='looker') }}

with quarters as ( -- get the first day of month from beg of 2018 through july 2022

    Select 
        distinct date_trunc('quarter', first_day_of_month) as first_day_of_quarter
    from {{ref('dim_dates')}} 
    where year(first_day_of_quarter) >= 2018
        and first_day_of_quarter < current_date

), exchange_rates as ( --get the exchange rates valid to/from dates
    Select 
        currency, 
        timestamp as valid_from, 
        lead(timestamp) 
            over (partition by currency order by timestamp) as valid_to, -- this is the next time the rate changes for that currency
        rate 
    from chipper.{{ var("core_public") }}.exchange_rates

), exchange_rate_at_BOM as ( --get the exchange rate at the Beg of each month
    
    Select 
        q.first_day_of_quarter, 
        er.currency, 
        er.valid_from, 
        er.valid_to, 
        er.rate as BOQ_rate 
    from exchange_rates er
    join quarters q -- this join gets the exchange rate at the BOM for each currency
        on q.first_day_of_quarter >= er.valid_from 
        and q.first_day_of_quarter < er.valid_to
    
), users as ( -- excludes all risk/fraud/invalid users 

    Select 
        distinct user_id    
    from {{ref('expanded_users')}} 
    where is_internal = False
        and is_valid_user = True 
        and is_admin = False
        and is_deleted = False 
        and is_blocked_by_flag = False
        and user_id not in (Select distinct user_id from {{ ref('risk_flags')}} ) -- excluding any user with any risk flag 

), BOQ_balances as ( -- get begining of month balances for each user and each currency 
    
    Select  
        q.first_day_of_quarter as beginning_of_quarter,
        l.main_party_user_id as user_id, 
        l.ledger_currency, 
        er.BOQ_rate, 
        sum(l.ledger_amount) as  BoQ_Balance, 
        BoQ_Balance * er.BOQ_rate as BoQ_Balance_in_USD -- get beg of month value in USD for each currency balance 
    from {{ref('expanded_ledgers')}}  l
    join quarters q 
        on q.first_day_of_quarter > l.ledger_timestamp -- sums any ledger amount that occured prior to the first day of month -- using ledger_timestamp for dead_end ledgers
    join exchange_rate_at_BOM er 
        on q.first_day_of_quarter = er.first_day_of_quarter -- gets the beg of month exchange rate for each currency 
            and l.ledger_currency = er.currency
    join users u
        on u.user_id = l.main_party_user_id
    Group by 1,2,3,4
    
), BOQ_balance_USD_per_user as ( -- gets BOM balance in USD across all currencies 

    Select 
        user_id, 
        beginning_of_quarter, 
        -- setting negative wallet balances to zero in line with this notion doc 
        -- https://www.notion.so/Finance-Reporting-Zeroing-negative-balance-ee31526706034268aa098be452fc0c2d
        Case 
            when sum(BoQ_Balance_in_USD) < 0 
                then 0
            else sum(BoQ_Balance_in_USD) 
        end as Total_BoQ_Balance_in_USD
    from BOQ_balances 
    group by 1, 2
    
) 
Select * 
from BOQ_balance_USD_per_user
