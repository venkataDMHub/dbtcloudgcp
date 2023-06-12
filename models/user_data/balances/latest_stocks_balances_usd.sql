select
    user_id,
    try_to_numeric(payload:equityValue::text, 10, 3) as latest_stocks_balance_usd
from {{ var("core_public") }}.stock_balance_history
where latest_stocks_balance_usd > 0
qualify row_number() over (partition by user_id order by updated_at desc) = 1
