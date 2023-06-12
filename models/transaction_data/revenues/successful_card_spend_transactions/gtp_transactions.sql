{{ config(materialized='ephemeral') }}

select *
from {{ref('card_spend_transactions')}}
where
	external_provider = 'GTP'
	and provider_details:BaseIIStatus::text = 'C'
