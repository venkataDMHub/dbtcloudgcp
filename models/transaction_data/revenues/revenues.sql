with all_revenues as (
    select * from {{ref('airtime_sales')}}
    union
    select * from {{ref('data_bundle_sales')}}
    union
    select * from {{ref('forex_fees')}}
    union
    select * from {{ref('bill_payment_commissions')}}
    union
    select * from {{ref('crypto_sales')}}
    union
    select * from {{ref('stock_trading_fees')}}
    union
    select * from {{ref('debit_card_spend_revenues')}}
    union
    select * from {{ref('network_api_percentage_fees')}}
    union
    select * from {{ref('non_forex_fees')}}
    union
    select * from {{ref('debit_card_processing_fees')}}
)

select
    dense_rank() over (
        order by
            transaction_updated_at,
            transfer_id,
            journal_id,
            fee_calculation_id,
            fee_config_id,
            forex_fee_calculation_id,
            transfer_quote_id,
            external_provider,
            external_provider_transaction_id,
            revenue_stream
    ) as row_number,
    *
from all_revenues
order by row_number
