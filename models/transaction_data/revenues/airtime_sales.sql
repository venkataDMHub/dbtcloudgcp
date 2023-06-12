{{ config(materialized='ephemeral') }}

select
    airtime_purchases.transfer_id,
    airtime_purchases.journal_id,
    null as fee_calculation_id,
    null as fee_config_id,
    null as forex_fee_calculation_id,
    null as transfer_quote_id,
    airtime_provider as external_provider,
    external_id as external_provider_transaction_id,
    hlo_created_at as transaction_created_at,
    hlo_updated_at as transaction_updated_at,
    'AIRTIME_SALES' as revenue_stream,

    currency as revenue_currency,
    null as exchange_rate_fee_percentage_in_decimals,
    null as commission_revenue_rate_in_decimals,
    receive_value as gross_revenues,
    discount_percentage / 100 as sales_discount_percentage_in_decimals,
    -((sales_discount_percentage_in_decimals) * gross_revenues) as sales_discount,
    gross_revenues + sales_discount as net_revenues,

    case when revenue_currency = destination_currency then destination_rate
        when revenue_currency = origin_currency then origin_rate
        else null
    end as rate_to_usd,
    gross_revenues * rate_to_usd as gross_revenues_in_usd,
    sales_discount * rate_to_usd as sales_discount_in_usd,
    net_revenues * rate_to_usd as net_revenues_in_usd,

    user_id as monetized_user_id
from "CHIPPER".{{ var("core_public") }}."AIRTIME_PURCHASES" as airtime_purchases
join "CHIPPER"."DBT_TRANSFORMATIONS"."EXPANDED_TRANSFERS" as expanded_transfers
		on airtime_purchases.transfer_id = expanded_transfers.transfer_id
where 
    airtime_purchases.status = 'COMPLETED'
    and reverse_transfer_id is null
