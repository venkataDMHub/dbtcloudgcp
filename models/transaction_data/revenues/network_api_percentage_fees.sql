{{ config(materialized='ephemeral') }}

{% set carry_first_pctg = "0.005" %}

with network_api_percentage_fees_without_transfer_quotes as (
    select 
        fee_calculations.transfer_id,
        expanded_transfers.journal_id as journal_id,
        fee_calculations.id as fee_calculation_id,
        fee_calculations.fee_config_id,
        null as forex_fee_calculation_id,
        null as transfer_quote_id,
        null as external_provider,
        null as external_provider_transaction_id,
        hlo_created_at as transaction_created_at,
        hlo_updated_at as transaction_updated_at,
        'NETWORK_API_PERCENTAGE_FEES' as revenue_stream,
        case 
            when transfer_type = 'NETWORK_API_C2B_SETTLED' then expanded_transfers.destination_currency
            when transfer_type = 'NETWORK_API_B2C_SETTLED' then expanded_transfers.origin_currency
        end as revenue_currency,
        null as exchange_rate_fee_percentage_in_decimals,
        null as commission_revenue_rate_in_decimals,
        fee_calculations.percentage_fee_amount as gross_revenues,
        null as sales_discount_percentage_in_decimals,
        null as sales_discount,
        gross_revenues as net_revenues,
        case 
            when transfer_type = 'NETWORK_API_C2B_SETTLED' then expanded_transfers.destination_rate
            when transfer_type = 'NETWORK_API_B2C_SETTLED' then expanded_transfers.origin_rate
        end as rate_to_usd,
        gross_revenues * rate_to_usd as gross_revenues_in_usd,
        sales_discount * rate_to_usd as sales_discount_in_usd,
        net_revenues * rate_to_usd as net_revenues_in_usd,
        case 
            when transfer_type = 'NETWORK_API_C2B_SETTLED' then incoming_user_id
            when transfer_type = 'NETWORK_API_B2C_SETTLED' then outgoing_user_id
        end as monetized_user_id
    from "CHIPPER".{{ var("core_public") }}."FEE_CALCULATIONS" as fee_calculations
    join "CHIPPER"."DBT_TRANSFORMATIONS"."EXPANDED_TRANSFERS" as expanded_transfers
        on fee_calculations.transfer_id = expanded_transfers.transfer_id
    left join "CHIPPER".{{ var("core_public") }}."TRANSFER_QUOTES" as transfer_quotes
        on expanded_transfers.transfer_id = transfer_quotes.transfer_id
    where 
        revenue_currency is not null
        and rate_to_usd is not null
        and monetized_user_id is not null
        and is_original_transfer_reversed = false
        and transfer_type in ('NETWORK_API_C2B_SETTLED', 'NETWORK_API_B2C_SETTLED')
        and transfer_quotes.transfer_id is null
),

network_api_percentage_fees_using_transfer_quotes as (
    select
        expanded_transfers.transfer_id,
        expanded_transfers.journal_id,
        fee_calculations.id as fee_calculation_id,
        fee_calculations.fee_config_id,
        null as forex_fee_calculation_id,
        transfer_quotes.id as transfer_quote_id,
        null as external_provider,
        null as external_provider_transaction_id,
        hlo_created_at as transaction_created_at,
        hlo_updated_at as transaction_updated_at,
        'NETWORK_API_PERCENTAGE_FEES' as revenue_stream,
        case 
            when transfer_type = 'NETWORK_API_C2B_SETTLED' then expanded_transfers.destination_currency
            when transfer_type = 'NETWORK_API_B2C_SETTLED' then expanded_transfers.origin_currency
        end as revenue_currency,
        null as exchange_rate_fee_percentage_in_decimals,
        null as commission_revenue_rate_in_decimals,
        fee_calculations.debited_amount as gross_revenues,
        null as sales_discount_percentage_in_decimals,
        null as sales_discount,
        gross_revenues as net_revenues,
        case 
            when transfer_type = 'NETWORK_API_C2B_SETTLED' then destination_rate
            when transfer_type = 'NETWORK_API_B2C_SETTLED' then origin_rate
        end as rate_to_usd,
        gross_revenues * rate_to_usd as gross_revenues_in_usd,
        sales_discount * rate_to_usd as sales_discount_in_usd,
        net_revenues * rate_to_usd as net_revenues_in_usd,
        case 
            when transfer_type = 'NETWORK_API_C2B_SETTLED' then incoming_user_id
            when transfer_type = 'NETWORK_API_B2C_SETTLED' then outgoing_user_id
        end as monetized_user_id
    from "CHIPPER".{{ var("core_public") }}."FEE_CALCULATIONS" as fee_calculations
    inner join "CHIPPER".{{ var("core_public") }}."TRANSFER_QUOTES" as transfer_quotes
        on fee_calculations.transfer_quote_id = transfer_quotes.id
    inner join "CHIPPER"."DBT_TRANSFORMATIONS"."EXPANDED_TRANSFERS" as expanded_transfers
        on transfer_quotes.transfer_id = expanded_transfers.transfer_id
    where
        is_original_transfer_reversed = false
        and transfer_type in ('NETWORK_API_C2B_SETTLED', 'NETWORK_API_B2C_SETTLED')
        and origin_amount_in_usd != destination_amount_in_usd
        and percentage_fee_amount = debited_amount
        and debited_amount != 0
),

carry_first_network_api_payments as (
        select 
        expanded_transfers.transfer_id,
        expanded_transfers.journal_id as journal_id,
        null as fee_calculation_id,
        null as fee_config_id,
        null as forex_fee_calculation_id,
        null as transfer_quote_id,
        null as external_provider,
        null as external_provider_transaction_id,
        hlo_created_at as transaction_created_at,
        hlo_updated_at as transaction_updated_at,
        'NETWORK_API_PERCENTAGE_FEES' as revenue_stream,
        case 
            when transfer_type = 'NETWORK_API_C2B_SETTLED' then expanded_transfers.destination_currency
            when transfer_type = 'NETWORK_API_B2C_SETTLED' then expanded_transfers.origin_currency
        end as revenue_currency,
        null as exchange_rate_fee_percentage_in_decimals,
        null as commission_revenue_rate_in_decimals,
        case 
            when transfer_type = 'NETWORK_API_C2B_SETTLED' then expanded_transfers.destination_amount * {{ carry_first_pctg }}
            when transfer_type = 'NETWORK_API_B2C_SETTLED' then expanded_transfers.origin_amount  * {{ carry_first_pctg }}
        end as gross_revenues,
        null as sales_discount_percentage_in_decimals,
        null as sales_discount,
        gross_revenues as net_revenues,
        case 
            when transfer_type = 'NETWORK_API_C2B_SETTLED' then expanded_transfers.destination_rate
            when transfer_type = 'NETWORK_API_B2C_SETTLED' then expanded_transfers.origin_rate
        end as rate_to_usd,
        gross_revenues * rate_to_usd as gross_revenues_in_usd,
        sales_discount * rate_to_usd as sales_discount_in_usd,
        net_revenues * rate_to_usd as net_revenues_in_usd,
        case 
            when transfer_type = 'NETWORK_API_C2B_SETTLED' then incoming_user_id
            when transfer_type = 'NETWORK_API_B2C_SETTLED' then outgoing_user_id
        end as monetized_user_id
    from "CHIPPER"."DBT_TRANSFORMATIONS"."EXPANDED_TRANSFERS" as expanded_transfers
    left join "CHIPPER".{{ var("core_public") }}."TRANSFER_QUOTES" as transfer_quotes
        on expanded_transfers.transfer_id = transfer_quotes.transfer_id
    where 
        revenue_currency is not null
        and rate_to_usd is not null
        and monetized_user_id = '07185dd0-3d78-11ec-ba22-7d2e386c38c9'
        and is_original_transfer_reversed = false
        and transfer_type in ('NETWORK_API_C2B_SETTLED', 'NETWORK_API_B2C_SETTLED')
        and transfer_quotes.transfer_id is null
)

select *
from network_api_percentage_fees_without_transfer_quotes

union

select *
from network_api_percentage_fees_using_transfer_quotes

union

select *
from carry_first_network_api_payments
