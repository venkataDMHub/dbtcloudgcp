{{ config(materialized='ephemeral') }}

select
	asset_trades.transfer_id,
	asset_trades.journal_id,
    null as fee_calculation_id,
    null as fee_config_id,
    null as forex_fee_calculation_id,
    null as transfer_quote_id,
	null as external_provider,
	null as external_provider_transaction_id,

	hlo_created_at as transaction_created_at,
	hlo_updated_at as transaction_updated_at,

	'CRYPTO_SALES' as revenue_stream,
	origin_currency as revenue_currency,
	null as exchange_rate_fee_percentage_in_decimals,
	null as commission_revenue_rate_in_decimals,
	origin_amount as gross_revenues,
	null as sales_discount_percentage_in_decimals,
	null as sales_discount,
	gross_revenues as net_revenues,

	origin_rate as rate_to_usd,
	gross_revenues * rate_to_usd as gross_revenues_in_usd,
    sales_discount * rate_to_usd as sales_discount_in_usd,
    net_revenues * rate_to_usd as net_revenues_in_usd,

	user_id as monetized_user_id
from {{ref('asset_trades')}}  
join "CHIPPER"."DBT_TRANSFORMATIONS"."EXPANDED_TRANSFERS" as expanded_transfers 
		on asset_trades.transfer_id = expanded_transfers.transfer_id
where 
	asset_trades.status = 'SETTLED'
	and position = 'BUY'
	and reverse_transfer_id is null
	and corridor = 'CRYPTO_TRADE'
