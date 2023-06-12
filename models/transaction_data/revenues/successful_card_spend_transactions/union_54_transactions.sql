{{ config(materialized='ephemeral') }}

select *
from {{ref('card_spend_transactions')}}
where
	external_provider = 'UNION54'
	and provider_details:status::text = 'settled'
