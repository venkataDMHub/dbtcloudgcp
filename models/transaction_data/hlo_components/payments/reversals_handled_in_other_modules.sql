{{ config(materialized='ephemeral') }}

{# /* Reverse Transfer IDs (refunds) of failed withdrawals 
    from ChipperIntelligence/dbt/models/transaction_data/hlo_components/withdrawals/withdrawals_with_all_transfer_ids.sql */ #}
select distinct transfer_id
from {{ ref('withdrawals_with_all_transfer_ids') }}
where is_transfer_reversal = true

union

{# /* Reverse Transfer IDs (refunds) of failed data purchases 
    from ChipperIntelligence/dbt/models/transaction_data/hlo_components/data_purchases/data_purchases_with_all_transfer_ids.sql */ #}
select distinct transfer_id
from {{ ref('data_purchases_with_all_transfer_ids') }}
where is_transfer_reversal = true
