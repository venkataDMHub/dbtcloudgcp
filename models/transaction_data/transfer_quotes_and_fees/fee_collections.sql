{{ config(materialized='table') }}

with ngn_parallel_rates as (
    select
        distinct date,
        rate,
        currency
    from chipper.utils.ngn_usd_parallel_market_rates
),

non_forex_fees as (
    select
        expanded_transfers.transfer_id,
        expanded_transfers.journal_id,
        fee_calculations.transfer_quote_id,
        fee_calculations.id as fee_calculation_id,
        fee_calculations.fee_config_id,
        null as forex_fee_calculation_id,

        iff(
            fee_calculations.flat_fee_amount = fee_calculations.debited_amount, 
            'FLAT_FEE', 
            'PERCENTAGE_FEE'
        ) as fee_type,
        fee_calculations.fee_name,
        fee_calculations.fee_description,
        
        fee_calculations.currency as fee_currency,
        fee_calculations.debited_amount as fee_amount_collected,
        iff(
            fee_calculations.currency = expanded_transfers.origin_currency, 
            origin_rate, 
            destination_rate
        ) as fee_rate_to_usd,
        fee_amount_collected * fee_rate_to_usd as fee_amount_collected_in_usd,
        ngn_parallel_rates.rate as fee_parallel_rate,
        iff(
            fee_calculations.currency = 'NGN', 
            (fee_amount_collected / fee_parallel_rate), 
            fee_amount_collected_in_usd
        ) as fee_amount_collected_in_usd_parallel,

        iff(fee_type = 'PERCENTAGE_FEE', fee_calculations.origin_amount, 0) as amount_percentage_fee_calculated_from,
        amount_percentage_fee_calculated_from * fee_rate_to_usd as amount_in_usd_percentage_fee_calculated_from,
        iff(
            fee_calculations.currency = 'NGN', 
            (amount_percentage_fee_calculated_from / fee_parallel_rate), 
            amount_in_usd_percentage_fee_calculated_from
        ) as amount_in_usd_parallel_percentage_fee_calculated_from,

        null as amount_forex_fee_calculated_from,
        null as amount_in_usd_forex_fee_calculated_from,
        null as amount_in_usd_parallel_forex_fee_calculated_from,
        null as forex_fee_percentage,
        null as base_modification_percentage,
        null as forex_fee_percentage_using_usd_parallel,

        expanded_transfers.origin_currency,
        expanded_transfers.destination_currency,
        concat_ws(
            '-', 
            expanded_transfers.origin_currency, 
            expanded_transfers.destination_currency
        ) as transaction_currency_pair,

        null as adjusted_exchange_rate_with_forex_fee,
        null as unadjusted_exchange_rate_without_forex_fee,

        case 
            when fee_calculations.user_role = 'PAYER' 
            and transfer_quotes.sender_id not in ({{internal_users()}}) and expanded_transfers.outgoing_user_id is not null
            and transfer_quotes.recipient_id not in ({{internal_users()}}) and expanded_transfers.incoming_user_id is not null
                then transfer_quotes.sender_id

            when fee_calculations.user_role = 'RECIPIENT' 
            and transfer_quotes.sender_id not in ({{internal_users()}}) and expanded_transfers.outgoing_user_id is not null
            and transfer_quotes.recipient_id not in ({{internal_users()}}) and expanded_transfers.incoming_user_id is not null
                then transfer_quotes.recipient_id

            else coalesce(expanded_transfers.outgoing_user_id, expanded_transfers.incoming_user_id)
        end as fee_payer_id,
        iff(fee_calculations.user_role = 'PAYER', 'ORIGIN', 'DESTINATION') as transactor_side,
        expanded_transfers.transfer_type,
        expanded_transfers.journal_type,
        transfer_quotes.activity_type,

        transfer_quotes.created_at as transfer_quote_created_at,
        transfer_quotes.updated_at as transfer_quote_updated_at,
        transfer_quotes.valid_until as transfer_quote_valid_until,
        expanded_transfers.hlo_created_at,
        expanded_transfers.hlo_updated_at,

        expanded_transfers.hlo_status,
        expanded_transfers.is_original_transfer_reversed,
        expanded_transfers.is_transfer_reversal,

        iff(revenues.transfer_id is not null, true, false) as is_gaap_revenue
    from {{ref('fee_calculations')}} as fee_calculations
    inner join {{ref('transfer_quotes')}} as transfer_quotes
        on fee_calculations.transfer_quote_id = transfer_quotes.id
    inner join chipper.dbt_transformations.expanded_transfers as expanded_transfers
        on transfer_quotes.transfer_id = expanded_transfers.transfer_id
    left join ngn_parallel_rates
        on cast(expanded_transfers.hlo_created_at as date) = ngn_parallel_rates.date
        and fee_calculations.currency = ngn_parallel_rates.currency
    left join chipper.dbt_transformations.revenues as revenues
        on transfer_quotes.transfer_id = revenues.transfer_id
        and fee_calculations.id = revenues.fee_calculation_id
        and fee_calculations.currency = revenues.revenue_currency
        and fee_calculations.debited_amount = revenues.gross_revenues
    where
        fee_calculations.debited_amount != 0
),

forex_fees_with_parallel as (
    select
        forex_fee_calculations.id as forex_fee_calculation_id,

         {# /* Origin side */ #}
        forex_fee_calculations.origin_currency as forex_fee_calculation_origin_currency,
        expanded_transfers.origin_rate as origin_rate_to_usd,
        origin_ngn_parallel_rates.rate as origin_parallel_rate,
        forex_fee_calculations.origin_amount as forex_fee_calculation_origin_amount,

             {# /* Origin in USD */ #}
        forex_fee_calculation_origin_amount * origin_rate_to_usd 
            as forex_fee_calculation_origin_amount_in_usd,

             {# /* Origin in USD parallel */ #}
        case when forex_fee_calculation_origin_currency = 'NGN'
            then forex_fee_calculation_origin_amount / origin_parallel_rate
            else forex_fee_calculation_origin_amount_in_usd
        end as forex_fee_calculation_origin_amount_in_usd_parallel,

         {# /* Destination side */ #}
        forex_fee_calculations.destination_currency as forex_fee_calculation_destination_currency,
        expanded_transfers.destination_rate as destination_rate_to_usd,
        destination_ngn_parallel_rates.rate as destination_parallel_rate,
        forex_fee_calculations.destination_amount as forex_fee_calculation_destination_amount,

             {# /* Destination in USD */ #}
        forex_fee_calculation_destination_amount * destination_rate_to_usd 
            as forex_fee_calculation_destination_amount_in_usd,

             {# /* Destination in USD parallel */ #}
        case when forex_fee_calculation_destination_currency = 'NGN'
            then forex_fee_calculation_destination_amount / destination_parallel_rate
            else forex_fee_calculation_destination_amount_in_usd
        end as forex_fee_calculation_destination_amount_in_usd_parallel

    from {{ref('forex_fee_calculations')}} as forex_fee_calculations
    inner join {{ref('transfer_quotes')}} as transfer_quotes
        on forex_fee_calculations.transfer_quote_id = transfer_quotes.id
    inner join chipper.dbt_transformations.expanded_transfers as expanded_transfers
        on transfer_quotes.transfer_id = expanded_transfers.transfer_id

    left join ngn_parallel_rates as origin_ngn_parallel_rates
        on cast(expanded_transfers.hlo_created_at as date) = origin_ngn_parallel_rates.date
        and forex_fee_calculations.fee_currency = origin_ngn_parallel_rates.currency

    left join ngn_parallel_rates as destination_ngn_parallel_rates
        on cast(expanded_transfers.hlo_created_at as date) = destination_ngn_parallel_rates.date
        and forex_fee_calculations.fee_currency = destination_ngn_parallel_rates.currency
),

forex_fees as (
    select
        expanded_transfers.transfer_id,
        expanded_transfers.journal_id,
        forex_fee_calculations.transfer_quote_id,
        null as fee_calculation_id,
        null as fee_config_id,
        forex_fee_calculations.id as forex_fee_calculation_id,

        'FOREX_FEE' as fee_type,
        'FOREX_FEE' as fee_name,
        'FOREX_FEE' as fee_description,

        forex_fee_calculations.fee_currency,
        forex_fee_calculations.debited_amount as fee_amount_collected,
        expanded_transfers.origin_rate as fee_rate_to_usd,
        forex_fee_calculation_origin_amount_in_usd - forex_fee_calculation_destination_amount_in_usd 
            as fee_amount_collected_in_usd,
        origin_parallel_rate as fee_parallel_rate,
        forex_fee_calculation_origin_amount_in_usd_parallel - forex_fee_calculation_destination_amount_in_usd_parallel
            as fee_amount_collected_in_usd_parallel,

        null as amount_percentage_fee_calculated_from,
        null as amount_in_usd_percentage_fee_calculated_from,
        null as amount_in_usd_parallel_percentage_fee_calculated_from,

        forex_fee_calculation_origin_amount as amount_forex_fee_calculated_from,
        forex_fee_calculation_origin_amount_in_usd as amount_in_usd_forex_fee_calculated_from,
        forex_fee_calculation_origin_amount_in_usd_parallel as amount_in_usd_parallel_forex_fee_calculated_from,
        forex_fee_calculations.fee_percentage as forex_fee_percentage,
        forex_fee_calculations.base_modification_percentage,
        (fee_amount_collected_in_usd_parallel / amount_in_usd_parallel_forex_fee_calculated_from) * 100
            as forex_fee_percentage_using_usd_parallel,

        expanded_transfers.origin_currency,
        expanded_transfers.destination_currency,
        concat_ws(
            '-', 
            expanded_transfers.origin_currency, 
            expanded_transfers.destination_currency
        ) as transaction_currency_pair,

        forex_fee_calculations.rate_with_fee as adjusted_exchange_rate_with_forex_fee,
        forex_fee_calculations.rate_without_fee as unadjusted_exchange_rate_without_forex_fee,

        coalesce(expanded_transfers.outgoing_user_id, expanded_transfers.incoming_user_id) as fee_payer_id,
        iff(fee_payer_id = expanded_transfers.outgoing_user_id, 'ORIGIN', 'DESTINATION') as transactor_side,
        expanded_transfers.transfer_type,
        expanded_transfers.journal_type,
        transfer_quotes.activity_type,

        transfer_quotes.created_at as transfer_quote_created_at,
        transfer_quotes.updated_at as transfer_quote_updated_at,
        transfer_quotes.valid_until as transfer_quote_valid_until,
        expanded_transfers.hlo_created_at,
        expanded_transfers.hlo_updated_at,

        expanded_transfers.hlo_status,
        expanded_transfers.is_original_transfer_reversed,
        expanded_transfers.is_transfer_reversal,

        iff(revenues.transfer_id is not null, true, false) as is_gaap_revenue
    from {{ref('forex_fee_calculations')}} as forex_fee_calculations
    inner join {{ref('transfer_quotes')}} as transfer_quotes
        on forex_fee_calculations.transfer_quote_id = transfer_quotes.id
    inner join chipper.dbt_transformations.expanded_transfers as expanded_transfers
        on transfer_quotes.transfer_id = expanded_transfers.transfer_id
    inner join forex_fees_with_parallel
        on forex_fee_calculations.id = forex_fees_with_parallel.forex_fee_calculation_id
    left join chipper.dbt_transformations.revenues as revenues
        on transfer_quotes.transfer_id = revenues.transfer_id
        and forex_fee_calculations.id = revenues.forex_fee_calculation_id
        and forex_fee_calculations.fee_currency = revenues.revenue_currency
        and forex_fee_calculations.debited_amount = revenues.gross_revenues
    where
        forex_fee_calculation_origin_amount_in_usd != forex_fee_calculation_destination_amount_in_usd
        or forex_fee_calculation_origin_amount_in_usd_parallel != forex_fee_calculation_destination_amount_in_usd_parallel
),

all_fee_collections as (
    select * from non_forex_fees
    union
    select * from forex_fees
)

select
    dense_rank() over (
        order by
            transfer_id,
            journal_id,
            transfer_quote_id,
            fee_calculation_id,
            fee_config_id,
            forex_fee_calculation_id
    ) as row_number,
    *
from all_fee_collections
order by transfer_id
