{{ config(materialized='table') }}

{% set valid_transfer_types = (
        'AIRTIME_PURCHASES_COMPLETED',
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
        'PAYMENTS_CARD_DECLINED_FEE_FULL_SETTLED',
        'PAYMENTS_CARD_DECLINED_FEE_PARTIAL_SETTLED',
        'PAYMENTS_CARD_ISSUANCE_FEE_SETTLED',
        'PAYMENTS_CARD_WITHDRAWAL_FEE_SETTLED',
        'PAYMENTS_P2P_SETTLED',
        'PAYMENT_INVITATIONS_SETTLED',
        'REQUESTS_SETTLED',
        'S2NC_SETTLED',
        'STOCK_TRADES_BUY_SETTLED',
        'STOCK_TRADES_DIV_SETTLED',
        'STOCK_TRADES_DIVTAX_SETTLED',
        'STOCK_TRADES_MERGER_EXCHANGE_STOCK_CASH_SETTLED',
        'STOCK_TRADES_SELL_SETTLED',
        'WITHDRAWALS_SETTLED'
    )
%}

{% set crypto_transfer_types = (
        'ASSET_TRADES_BUY_SETTLED',
        'ASSET_TRADES_SELL_SETTLED',
        'CRYPTO_DEPOSITS_SETTLED',
        'CRYPTO_WITHDRAWALS_SETTLED'
    )
%}

{% set crypto_trade_transfer_types = (
        'ASSET_TRADES_BUY_SETTLED',
        'ASSET_TRADES_SELL_SETTLED'
    )
%}

{% set stock_trade_transfer_types = (
        'STOCK_TRADES_BUY_SETTLED',
        'STOCK_TRADES_DIV_SETTLED',
        'STOCK_TRADES_DIVTAX_SETTLED',
        'STOCK_TRADES_MERGER_EXCHANGE_STOCK_CASH_SETTLED',
        'STOCK_TRADES_SELL_SETTLED'
    )
%}

{% set p2p_transfer_types = (
        'PAYMENTS_P2P_SETTLED',
        'PAYMENT_INVITATIONS_SETTLED',
        'REQUESTS_SETTLED'
    )
%}

{% set p2p_send_transfer_types = (
        'PAYMENTS_P2P_SETTLED',
        'PAYMENT_INVITATIONS_SETTLED'
    )
%}

{% set airtime_data_transfer_types = (
        'AIRTIME_PURCHASES_COMPLETED',
        'DATA_PURCHASES_COMPLETED'
    )
%}

with clean_record_users as (
    select user_id
    from {{ ref('expanded_users') }} as expanded_users
    where
        is_valid_user = true
        and is_deleted = false
        and is_blocked_by_flag = false
        and has_risk_flag = false
),

monetized_users as (
    select monetized_user_id as user_id
    from {{ ref('user_demographic_is_monetized_user') }}
),

users_with_at_least_3_valid_transactions as (
    select main_party_user_id as user_id
    from {{ ref('expanded_ledgers') }} as expanded_ledgers
    where
        is_original_transfer_reversed = false
        and transfer_type in {{valid_transfer_types}}
    qualify count(distinct transfer_id) over (partition by main_party_user_id) >= 3
),

all_bod_user_ids as (
    select * from clean_record_users
    union
    select * from monetized_users
    union
    select * from users_with_at_least_3_valid_transactions
),

transactors_with_type as (
    select
        distinct main_party_user_id,

        max(case when transfer_type = 'DEPOSITS_SETTLED' then true else false end) as is_deposit_user,
        max(case when transfer_type = 'WITHDRAWALS_SETTLED' then true else false end) as is_withdrawal_user,

        max(case when (transfer_type in {{crypto_transfer_types}} and corridor in ('LOCAL_CRYPTO', 'CRYPTO_TRADE')) then true else false end) as is_crypto_user,
        max(case when (transfer_type in {{crypto_trade_transfer_types}} and corridor = 'CRYPTO_TRADE') then true else false end) as is_crypto_trade_user,
        max(case when transfer_type in {{stock_trade_transfer_types}} then true else false end) as is_stock_trade_user,

        max(case when transfer_type in {{p2p_transfer_types}} then true else false end) as is_p2p_user,
        max(case when transfer_type in {{p2p_send_transfer_types}} then true else false end) as is_p2p_send_user,
        max(case when transfer_type = 'REQUESTS_SETTLED' then true else false end) as is_p2p_request_user,

        max(case when transfer_type in {{airtime_data_transfer_types}} then true else false end) as is_airtime_and_data_user,
        max(case when transfer_type = 'AIRTIME_PURCHASES_COMPLETED' then true else false end) as is_airtime_user,
        max(case when transfer_type = 'BILL_PAYMENTS_COMPLETED' then true else false end) as is_bill_pay_user,

        max(case when transfer_type = 'S2NC_SETTLED' then true else false end) as is_s2nc_user

    from {{ ref('expanded_ledgers') }} as expanded_ledgers
    where 
        is_original_transfer_reversed = false
        and transfer_type in {{valid_transfer_types}}
    group by main_party_user_id
)

select
    user_demographic_features.user_id,
    primary_currency,
    acquisition_source,
    kyc_tier,
    acquisition_date,
    is_monetized_user,
    iff(transactors_with_type.main_party_user_id is not null, true, false) as is_transacting_user,
    ifnull(is_deposit_user, false) as is_deposit_user,
    ifnull(is_withdrawal_user, false) as is_withdrawal_user,
    ifnull(is_crypto_user, false) as is_crypto_user,
    ifnull(is_crypto_trade_user, false) as is_crypto_trade_user,
    ifnull(is_stock_trade_user, false) as is_stock_trade_user,
    ifnull(is_p2p_user, false) as is_p2p_user,
    ifnull(is_p2p_send_user, false) as is_p2p_send_user,
    ifnull(is_p2p_request_user, false) as is_p2p_request_user,
    ifnull(is_airtime_and_data_user, false) as is_airtime_and_data_user,
    ifnull(is_airtime_user, false) as is_airtime_user,
    ifnull(is_bill_pay_user, false) as is_bill_pay_user,
    ifnull(is_s2nc_user, false) as is_s2nc_user
from {{ ref('user_demographic_features') }} as user_demographic_features
inner join all_bod_user_ids on user_demographic_features.user_id = all_bod_user_ids.user_id
left join transactors_with_type on user_demographic_features.user_id = transactors_with_type.main_party_user_id
where user_demographic_features.user_id not in ({{internal_users()}})
