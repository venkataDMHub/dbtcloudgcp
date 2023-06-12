{{ config(materialized='ephemeral') }}

with combined_debit_card_transactions as (
    select * from {{ref('gtp_transactions')}}
    union
    select * from {{ref('union_54_transactions')}}
)

select
	transfer_id,
	journal_id,
    fee_calculation_id,
    fee_config_id,
    forex_fee_calculation_id,
    transfer_quote_id,
	external_provider,
	external_provider_transaction_id,
	transaction_created_at,
	transaction_updated_at,
	revenue_stream,
	
    revenue_currency,
	exchange_rate_fee_percentage_in_decimals,
	commission_revenue_rate_in_decimals,
	gross_revenues,	
	sales_discount_percentage_in_decimals,
	sales_discount,
	net_revenues,
	
    rate_to_usd,
	gross_revenues_in_usd,
	sales_discount_in_usd,
	net_revenues_in_usd,
	
	monetized_user_id
from combined_debit_card_transactions
