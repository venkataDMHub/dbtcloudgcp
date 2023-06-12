{{ config(materialized='ephemeral') }}


select
    c.user_id,
    ifnull(s.latest_stocks_balance_usd, 0) as total_stock_balance_usd,
    ifnull(c.total_balance_usd, 0) as total_crypto_and_fiat_balance,
    total_stock_balance_usd + total_crypto_and_fiat_balance as total_balance_usd
from {{ ref("latest_wallet_balances_usd") }} c
full outer join {{ ref("latest_stocks_balances_usd") }} s on s.user_id = c.user_id
where total_balance_usd > 0
