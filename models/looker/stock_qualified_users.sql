{{ config(materialized='table', dist='user_id', schema='looker') }}

{% set STOCK_PRIMARY_CURRENCIES_LIST = ('NGN', 'UGX') 
%}


with qualified_users as (

    select
        users.user_id,
        users.nationality,
        users.dob,
        users.user_age,
        users.gender,
        users.primary_currency,
        users.acquisition_source,
        users.acquisition_date,
        users.kyc_tier,
        users.purpose_of_account,
        users.account_age as account_age_days,
        users.is_internal,
        users.is_admin,
        users.is_business,
        users.is_valid_user,
        users.has_risk_flag,
        users.is_deleted,
        users.invalid_user_reasons,
        users.is_blocked_by_flag,
        users.all_active_flags,
        users.num_flags,
        users.postal_code_latest,
        users.uos_score,
        users.total_lpv_usd,
        users.percent_lpv_from_rewards,
        users.lpv_group,
        users.lpv_range_min,
        users.lpv_range_max,
        users.latest_engagement_bucket,
        users.latest_engagement_score,
        accounts.user_status as stock_user_status,
        accounts.created_at as stock_onboarding_started_at,
        case
            when users.uos_score >= 0.8 then 'High Risk'
            when users.uos_score > 0.3 then 'Medium Risk'
            when users.uos_score <= 0.3 then 'Low Risk'
            else 'NOT_AVAILABLE'
        end as risk_bucket,
        case
            when accounts.user_status is not null
                then TRUE
            else FALSE
        end as has_landed_on_stocks_page,
        case
            when accounts.user_status in ('ACCEPTED', 'NEW')
                then TRUE
            else FALSE
        end as has_started_stock_onboarding,
        case
            when accounts.user_status = 'ACCEPTED'
                then TRUE
            else FALSE
        end as has_completed_stock_onboarding,
        COALESCE(accounts.onboarded_at, accounts.created_at) as stock_onboarding_completed_at,
        COALESCE(balance.total_fiat_balance_usd, 0) as total_fiat_balance_usd,
        COALESCE(balance.total_crypto_balance_usd, 0) as total_crypto_balance_usd,
        COALESCE(balance.total_balance_usd, 0) as total_balance_usd

    from {{ref('user_demographic_features')}} as users
    left join chipper.{{ var("core_public") }}.stock_accounts as accounts
        on users.user_id = accounts.user_id
    left join {{ref('latest_wallet_balances_usd')}} as balance
        on users.user_id = balance.user_id
    where users.primary_currency in {{STOCK_PRIMARY_CURRENCIES_LIST}}


),

stock_activity as (

    select
        ledgers.main_party_user_id as user_id,
        COUNT(
            distinct IFF(
                ledgers.transfer_type = 'STOCK_TRADES_BUY_SETTLED', ledgers.transfer_id, NULL
            )
        ) as stock_trades_buy_transactions,
        MIN(
            distinct IFF(
                ledgers.transfer_type = 'STOCK_TRADES_BUY_SETTLED', ledgers.hlo_updated_at, NULL
            )
        ) as first_stock_bought_at,
        MAX(
            distinct IFF(
                ledgers.transfer_type = 'STOCK_TRADES_BUY_SETTLED', ledgers.hlo_updated_at, NULL
            )
        ) as last_stock_bought_at,

        COUNT(
            distinct IFF(
                ledgers.transfer_type = 'STOCK_TRADES_SELL_SETTLED', ledgers.transfer_id, NULL
            )
        ) as stock_trades_sell_transactions,
        MIN(
            distinct IFF(
                ledgers.transfer_type = 'STOCK_TRADES_SELL_SETTLED', ledgers.hlo_updated_at, NULL
            )
        ) as first_stock_sold_at,
        MAX(
            distinct IFF(
                ledgers.transfer_type = 'STOCK_TRADES_SELL_SETTLED', ledgers.hlo_updated_at, NULL
            )
        ) as last_stock_sold_at,

        COUNT(
            distinct IFF(
                ledgers.transfer_type = 'STOCK_TRADES_DIV_SETTLED', ledgers.transfer_id, NULL
            )
        ) as stock_trades_div_transactions,
        COUNT(
            distinct IFF(
                ledgers.transfer_type = 'STOCK_TRADES_REWARD_SETTLED', ledgers.transfer_id, NULL
            )
        ) as stock_trades_reward_transactions,
        COUNT(
            distinct IFF(
                ledgers.transfer_type = 'STOCK_TRADES_DIVTAX_SETTLED', ledgers.transfer_id, NULL
            )
        ) as stock_trades_divtax_transactions

    from {{ref('expanded_ledgers')}} as ledgers
    where ledgers.transfer_type like '%STOCK%'
        and ledgers.hlo_status in (
            'COMPLETED', 'SETTLED'
        )
        and ledgers.is_original_transfer_reversed = FALSE
    group by user_id

),

ledger_cleanup as (

    select
        ledgers.main_party_user_id,
        ledgers.transfer_id,
        ledgers.journal_id,
        ledgers.hlo_created_at,
        ABS(ledgers.ledger_amount_in_usd) as ledger_amount_in_usd,
        case
            when ledgers.transfer_type like '%_SETTLED%'
                then REGEXP_REPLACE(ledgers.transfer_type, '_SETTLED', '')
            when ledgers.transfer_type like '%_COMPLETED%'
                then REGEXP_REPLACE(ledgers.transfer_type, '_COMPLETED', '')
        end as transfer_type_simple,
        case
            when ledgers.transfer_type in ('NETWORK_API_B2C_SETTLED', 'NETWORK_API_C2B_SETTLED')
                and ledgers.ledger_amount_in_usd < 0
                then CONCAT(transfer_type_simple, '_SENT')
            when ledgers.transfer_type in ('NETWORK_API_B2C_SETTLED', 'NETWORK_API_C2B_SETTLED')
                and ledgers.ledger_amount_in_usd > 0
                then CONCAT(transfer_type_simple, '_RECEIVED')
            when
                ledgers.transfer_type in (
                    'PAYMENTS_P2P_SETTLED', 'PAYMENT_INVITATIONS_SETTLED', 'REQUESTS_SETTLED'
                )
                and ledgers.ledger_amount_in_usd < 0
                and ledgers.corridor = 'CROSS_BORDER_FIAT'
                then CONCAT(transfer_type_simple, '_SENT_CROSS_BORDER')
            when
                ledgers.transfer_type in (
                    'PAYMENTS_P2P_SETTLED', 'PAYMENT_INVITATIONS_SETTLED', 'REQUESTS_SETTLED'
                )
                and ledgers.ledger_amount_in_usd < 0
                and ledgers.corridor = 'LOCAL_FIAT'
                then CONCAT(transfer_type_simple, '_SENT_LOCAL')
            when
                ledgers.transfer_type in (
                    'PAYMENTS_P2P_SETTLED', 'PAYMENT_INVITATIONS_SETTLED', 'REQUESTS_SETTLED'
                )
                and ledgers.ledger_amount_in_usd > 0
                and ledgers.corridor = 'CROSS_BORDER_FIAT'
                then CONCAT(transfer_type_simple, '_RECEIVED_CROSS_BORDER')
            when
                ledgers.transfer_type in (
                    'PAYMENTS_P2P_SETTLED', 'PAYMENT_INVITATIONS_SETTLED', 'REQUESTS_SETTLED'
                )
                and ledgers.ledger_amount_in_usd > 0
                and ledgers.corridor = 'LOCAL_FIAT'
                then 'P2P_RECEIVED_LOCAL'
            else transfer_type_simple
        end as transfer_type_clean
    from {{ref('expanded_ledgers')}} as ledgers
    where
        ledgers.hlo_status in (
            'COMPLETED', 'SETTLED'
        ) and ledgers.is_original_transfer_reversed = FALSE

),

activity_prior_to_onboarded as (

    select
        accounts.user_id,
        COUNT(
            distinct IFF(
                l_before.transfer_type_clean in ('ASSET_TRADES_BUY', 'ASSET_TRADES_SELL'),
                l_before.journal_id,
                l_before.transfer_id
            )
        ) as ledger_transactions_before_stock_onboarding_started,
        LISTAGG(
            distinct l_before.transfer_type_clean
        )as ledger_activities_before_stock_onboarding_started,
        SUM(
            IFF(
                l_before.transfer_type_clean in ('ASSET_TRADES_BUY', 'ASSET_TRADES_SELL'),
                l_before.ledger_amount_in_usd / 2,
                l_before.ledger_amount_in_usd
            )
        ) as total_lpv_prior_to_stock_onboarding_started

    from chipper.{{ var("core_public") }}.stock_accounts as accounts
    left join ledger_cleanup as l_before
        on accounts.user_id = l_before.main_party_user_id
            and l_before.hlo_created_at < accounts.created_at
    group by accounts.user_id

),

activity_after_onboarded as (

    select
        accounts.user_id,
        COUNT(
            distinct IFF(
                l_after.transfer_type_clean in ('ASSET_TRADES_BUY', 'ASSET_TRADES_SELL'),
                l_after.journal_id,
                l_after.transfer_id
            )
        ) as ledger_transactions_after_stock_onboarding_started,
        LISTAGG(
            distinct l_after.transfer_type_clean
        ) as ledger_activities_after_stock_onboarding_started,
        SUM(
            IFF(
                l_after.transfer_type_clean in ('ASSET_TRADES_BUY', 'ASSET_TRADES_SELL'),
                l_after.ledger_amount_in_usd / 2,
                l_after.ledger_amount_in_usd
            )
        ) as total_lpv_after_stock_onboarding_started
    from chipper.{{ var("core_public") }}.stock_accounts as accounts
    left join ledger_cleanup as l_after
        on accounts.user_id = l_after.main_party_user_id
            and l_after.hlo_created_at > accounts.created_at
    group by accounts.user_id

),

final as (

    select
        users.user_id,
        users.nationality,
        users.dob,
        users.user_age,
        users.gender,
        users.primary_currency,
        users.acquisition_source,
        users.acquisition_date,
        users.kyc_tier,
        users.purpose_of_account,
        users.account_age_days,
        users.is_internal,
        users.is_business,
        users.is_valid_user,
        users.is_admin,
        users.has_risk_flag,
        users.is_deleted,
        users.invalid_user_reasons,
        users.is_blocked_by_flag,
        users.all_active_flags,
        users.num_flags,
        users.postal_code_latest,
        users.uos_score,
        users.risk_bucket,
        users.total_lpv_usd,
        users.percent_lpv_from_rewards,
        users.lpv_group,
        users.lpv_range_min,
        users.lpv_range_max,
        users.latest_engagement_bucket,
        users.latest_engagement_score,
        users.stock_user_status,
        users.has_landed_on_stocks_page,
        users.has_started_stock_onboarding,
        users.has_completed_stock_onboarding,
        users.stock_onboarding_started_at,
        users.stock_onboarding_completed_at,
        users.total_fiat_balance_usd,
        users.total_crypto_balance_usd,
        users.total_balance_usd,
        stock.first_stock_bought_at,
        stock.last_stock_bought_at,
        stock.first_stock_sold_at,
        stock.last_stock_sold_at,
        prior.ledger_transactions_before_stock_onboarding_started,
        prior.ledger_activities_before_stock_onboarding_started,
        prior.total_lpv_prior_to_stock_onboarding_started,
        after.ledger_transactions_after_stock_onboarding_started,
        after.ledger_activities_after_stock_onboarding_started,
        after.total_lpv_after_stock_onboarding_started,
        COALESCE(stock.stock_trades_buy_transactions, 0) as stock_trades_buy_transactions,
        COALESCE(stock.stock_trades_sell_transactions, 0) as stock_trades_sell_transactions,
        COALESCE(stock.stock_trades_div_transactions, 0) as stock_trades_div_transactions,
        COALESCE(stock.stock_trades_reward_transactions, 0) as stock_trades_reward_transactions,
        COALESCE(stock.stock_trades_divtax_transactions, 0) as stock_trades_divtax_transactions
    from qualified_users as users
    left join stock_activity as stock
        on users.user_id = stock.user_id
    left join activity_prior_to_onboarded as prior
        on users.user_id = prior.user_id
    left join activity_after_onboarded as after
        on users.user_id = after.user_id
)

select *
from final
