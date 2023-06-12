{{ config(materialized='ephemeral') }}

select
    transfer_id,
    journal_id,
    null as fee_calculation_id,
    null as fee_config_id,
    null as forex_fee_calculation_id,
    null as transfer_quote_id,
    null as external_provider,
    null as external_provider_transaction_id,
    hlo_created_at as transaction_created_at,
    hlo_updated_at as transaction_updated_at,
    'CARD_PROCESSING_FEES' as revenue_stream,
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
    outgoing_user_id as monetized_user_id
from "CHIPPER"."DBT_TRANSFORMATIONS"."EXPANDED_TRANSFERS" as expanded_transfers
where 
    transfer_type in ('PAYMENTS_CARD_DECLINED_FEE_FULL_SETTLED', 'PAYMENTS_CARD_DECLINED_FEE_PARTIAL_SETTLED', 'PAYMENTS_CARD_ISSUANCE_FEE_SETTLED', 'PAYMENTS_CARD_WITHDRAWAL_FEE_SETTLED')
    and is_original_transfer_reversed = false
