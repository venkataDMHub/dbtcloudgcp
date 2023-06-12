{{ config(materialized='ephemeral') }}

{% set hardcoded_exchange_rates = { 
	'GHS': 0.13,
	'NGN': 0.0024,
	'KES': 0.0087,
    'UGX': 0.00028,
    'RWF': 0.00098,
    'TZS': 0.00043,
    'ZAR': 0.069,
	'USD': 1.00
	} 
%}

{% set COMMISSION_REVENUE_RATE = 0.0475 %}

with daily_exchange_rates as (
	select 
		currency, 
		max(rate) as max_daily_rate_to_usd, 
		date_trunc('day', timestamp) as timestamp_day
	from "CHIPPER".{{ var("core_public") }}."EXCHANGE_RATES" as exchange_rates
	group by 
		currency, 
        timestamp_day
)

select
	transfer_id,
	journal_id,
    null as fee_calculation_id,
    null as fee_config_id,
    null as forex_fee_calculation_id,
    null as transfer_quote_id,
	card_issuer as external_provider,
	provider_transaction_id::varchar as external_provider_transaction_id,
	convert_timezone('UTC', timestamp) as transaction_created_at,
	convert_timezone('UTC', timestamp) as transaction_updated_at,
	'CARD_SPEND_FEES_ESTIMATED' as revenue_stream,
	
	issued_card_transactions.provider_details as provider_details,

    issued_card_transactions.currency as revenue_currency,
	null as exchange_rate_fee_percentage_in_decimals,
	{{ COMMISSION_REVENUE_RATE }} as commission_revenue_rate_in_decimals,
	amount * commission_revenue_rate_in_decimals as gross_revenues,	
	null as sales_discount_percentage_in_decimals,
	null as sales_discount,
	gross_revenues as net_revenues,
	
    max (
		case
        	{% for currency, rate in hardcoded_exchange_rates.items() %}
				when max_daily_rate_to_usd is null
		    		and revenue_currency = '{{currency}}' then {{ rate }}
			{% endfor %}
		    	else max_daily_rate_to_usd
		end
	) as rate_to_usd,
	gross_revenues * rate_to_usd as gross_revenues_in_usd,
	sales_discount * rate_to_usd as sales_discount_in_usd,
	net_revenues * rate_to_usd as net_revenues_in_usd,
	
	issued_card_transactions.user_id as monetized_user_id
from "CHIPPER".{{ var("core_public") }}."ISSUED_CARD_TRANSACTIONS" as issued_card_transactions
left join "CHIPPER".{{ var("core_public") }}."ISSUED_CARDS" as issued_cards
        on (
            issued_card_transactions.card_id = issued_cards.id 
            and issued_card_transactions.provider_card_id = issued_cards.provider_card_id
        )
left join daily_exchange_rates
		on (
			issued_card_transactions.currency = daily_exchange_rates.currency
			and date_trunc('day', transaction_updated_at) = daily_exchange_rates.timestamp_day
		)
where type = 'TRANSACTION'
group by
	transfer_id,
	journal_id,
	external_provider,
	external_provider_transaction_id,
	transaction_created_at,
	transaction_updated_at,
	revenue_stream,
	issued_card_transactions.provider_details,
	revenue_currency,
	exchange_rate_fee_percentage_in_decimals,
	commission_revenue_rate_in_decimals,
	gross_revenues,
	sales_discount_percentage_in_decimals,
	sales_discount,
	monetized_user_id
