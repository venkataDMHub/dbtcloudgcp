{{ config(materialized='ephemeral') }}

Select l.MAIN_PARTY_USER_ID as user_id, 
        l.LEDGER_CURRENCY as currency, 
        er.RATE as latest_available_exchange_rate,
        sum(l.LEDGER_AMOUNT) as latest_wallet_balance,
        max(l.ledger_timestamp) as latest_balance_updated_at,
        latest_wallet_balance * latest_available_exchange_rate  as latest_wallet_balance_in_usd
from {{ ref('expanded_ledgers') }} l
left join {{ ref('most_recent_exchange_rates') }} er
  on l.LEDGER_CURRENCY = er.CURRENCY
where l.MAIN_PARTY_USER_ID not in ({{internal_users()}})
Group by l.MAIN_PARTY_USER_ID, l.LEDGER_CURRENCY, er.RATE
