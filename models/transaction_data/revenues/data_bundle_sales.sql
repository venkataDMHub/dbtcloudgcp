{{ config(materialized='ephemeral') }}

select 
    data_purchases.transfer_id, 
    data_purchases.journal_id,
    null as fee_calculation_id,
    null as fee_config_id,
    null as forex_fee_calculation_id,
    null as transfer_quote_id,
    data_purchases.data_provider as external_provider,
	data_purchases.external_id as external_provider_transaction_id,
    hlo_created_at as transaction_created_at,
	hlo_updated_at as transaction_updated_at,
    'DATA_BUNDLE_SALES' as revenue_stream,
    
    data_purchases.currency as revenue_currency,
	null as exchange_rate_fee_percentage_in_decimals,
	null as commission_revenue_rate_in_decimals,
    data_purchases.amount as gross_revenues,
	null as sales_discount_percentage_in_decimals,
	null as sales_discount,
	gross_revenues as net_revenues,
    
    case when revenue_currency = destination_currency then destination_rate
        when revenue_currency = origin_currency then origin_rate
        else null
    end as rate_to_usd,
    gross_revenues * rate_to_usd as gross_revenues_in_usd,
    sales_discount * rate_to_usd as sales_discount_in_usd,
    net_revenues * rate_to_usd as net_revenues_in_usd,
    
    data_purchases.user_id as monetized_user_id
from "CHIPPER".{{ var("core_public") }}."DATA_PURCHASES" as data_purchases
join "CHIPPER"."DBT_TRANSFORMATIONS"."EXPANDED_TRANSFERS" as expanded_transfers
        on data_purchases.transfer_id = expanded_transfers.transfer_id
where 
    data_purchases.status = 'COMPLETED'
    and data_purchases.reverse_transfer_id is NULL
