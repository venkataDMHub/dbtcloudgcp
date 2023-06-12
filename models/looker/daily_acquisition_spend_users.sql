{{ config(materialized='table', schema='looker') }}

with dim_dates as (

    Select distinct date 
    from {{ ref('dim_dates') }} 
    where date <= current_date

), 

referral_spend as (

    select
        date_trunc('day', hlo_updated_at) as date,
        sum(abs(ledger_amount_in_usd)) as referral_spend
    from {{ ref('expanded_ledgers') }}
    where transfer_type like '%REFERRAL%'
        and hlo_status = 'SETTLED' 
    group by
        date

),

ad_spend as (

    select
        date_trunc('day', date) as date,
        sum(total_cost) as ad_spend
    from {{ ref('branch_acquisition_costs') }}
    group by date

),

users_acquired as (

    select
        date_trunc('day', created_at) as date,
        count(distinct user_id) as total_users_acquired
    from {{ ref('expanded_users') }}
    where is_internal = False
        and is_admin = False
    group by date

),

first_time_transactors as (

    select 
        date_trunc('day', users.created_at) as date,
        count(distinct users.user_id) as users_with_transactions
    from {{ ref('expanded_users') }} as users
    inner join {{ ref('expanded_ledgers') }} as ledgers
        on users.user_id = ledgers.main_party_user_id 
    where
        ledgers.hlo_status in ('COMPLETED', 'SETTLED')
        and ledgers.is_original_transfer_reversed = False
        and transfer_type in ('AIRTIME_PURCHASES_COMPLETED',
                        'ASSET_TRADES_BUY_SETTLED',
                        'ASSET_TRADES_SELL_SETTLED',
                        'BILL_PAYMENTS_COMPLETED',
                        'CHECKOUTS_SETTLED',
                        'CRYPTO_DEPOSITS_SETTLED',
                        'CRYPTO_WITHDRAWALS_SETTLED',
                        'DATA_PURCHASES_COMPLETED',
                        'DEPOSITS_SETTLED',
                        'ISSUED_CARD_TRANSACTIONS_FUNDING_COMPLETED',
                        'ISSUED_CARD_TRANSACTIONS_WITHDRAWAL_COMPLETED',
                        'NETWORK_API_B2C_SETTLED',
                        'NETWORK_API_C2B_SETTLED',
                        'PAYMENTS_P2P_SETTLED',
                        'PAYMENT_INVITATIONS_SETTLED',
                        'REQUESTS_SETTLED',
                        'STOCK_TRADES_BUY_SETTLED',
                        'STOCK_TRADES_DIVTAX_SETTLED',
                        'STOCK_TRADES_DIV_SETTLED',
                        'STOCK_TRADES_SELL_SETTLED',
                        'WITHDRAWALS_SETTLED')
        and users.is_internal = False
        and users.is_admin = False
    Group by date

),

first_time_depositors as (

    select
        date_trunc('day', users.created_at) as date,
        count(distinct user_id) as users_with_deposits
    from {{ ref('expanded_users') }} as users
    inner join  {{ ref('expanded_ledgers') }}  as ledgers
        on users.user_id = ledgers.main_party_user_id
    where
        ledgers.hlo_status in ('COMPLETED', 'SETTLED')
        and ledgers.is_original_transfer_reversed = False
        and ledgers.transfer_type = 'DEPOSITS_SETTLED'
        and users.is_internal = False
        and users.is_admin = False
    Group by date

),

final as (

    select
        dates.date, 
        coalesce(referral.referral_spend, 0) as total_referral_spend,
        coalesce(ads.ad_spend, 0) as total_ad_spend,
        total_referral_spend + total_ad_spend as total_spend,
        coalesce(users.total_users_acquired, 0) as users_acquired,
        coalesce(transactors.users_with_transactions, 0) as users_with_transactions,
        coalesce(depositors.users_with_deposits,0) as users_with_deposits
    from dim_dates dates
    left join referral_spend as referral
        on dates.date =  referral.date
    left join ad_spend as ads
        on dates.date = ads.date
    left join users_acquired as users
        on dates.date = users.date
    left join first_time_transactors as transactors
        on dates.date = transactors.date
    left join first_time_depositors depositors
        on dates.date = depositors.date
)

select *
from final
