{{ config(materialized='table') }}

with ngn_parallel_rates as (
    select
        distinct date,
        rate,
        currency
    from chipper.utils.ngn_usd_parallel_market_rates
),

aggregated_fee_collections as (
    select
        transfer_quote_id,
        transfer_id,
        transaction_currency_pair,

        {# /* Origin side */ #}
        origin_currency,

            {# /* Origin flat fees */ #}
        sum(
            case when transactor_side = 'ORIGIN' and fee_type = 'FLAT_FEE'
                then fee_amount_collected else 0 end
        ) as total_origin_flat_fees_collected,
        sum(
            case when transactor_side = 'ORIGIN' and fee_type = 'FLAT_FEE'
                then fee_amount_collected_in_usd else 0 end
        ) as total_origin_flat_fees_collected_in_usd,
        sum(
            case when transactor_side = 'ORIGIN' and fee_type = 'FLAT_FEE'
                then fee_amount_collected_in_usd_parallel else 0 end
        ) as total_origin_flat_fees_collected_in_usd_parallel,

            {# /* Origin percentage fees */ #}
        max(
            case when transactor_side = 'ORIGIN' and fee_type = 'PERCENTAGE_FEE' 
                then amount_percentage_fee_calculated_from else 0 end
        ) as origin_amount_percentage_fee_calculated_from,
        max(
            case when transactor_side = 'ORIGIN' and fee_type = 'PERCENTAGE_FEE' 
                then amount_in_usd_percentage_fee_calculated_from else 0 end
        ) as origin_amount_in_usd_percentage_fee_calculated_from,
        max(
            case when transactor_side = 'ORIGIN' and fee_type = 'PERCENTAGE_FEE' 
                then amount_in_usd_parallel_percentage_fee_calculated_from else 0 end
        ) as origin_amount_in_usd_parallel_percentage_fee_calculated_from,
        sum(
            case when transactor_side = 'ORIGIN' and fee_type = 'PERCENTAGE_FEE'
                then fee_amount_collected else 0 end
        ) as total_origin_percentage_fees_collected,
        sum(
            case when transactor_side = 'ORIGIN' and fee_type = 'PERCENTAGE_FEE'
                then fee_amount_collected_in_usd else 0 end
        ) as total_origin_percentage_fees_collected_in_usd,
        sum(
            case when transactor_side = 'ORIGIN' and fee_type = 'PERCENTAGE_FEE'
                then fee_amount_collected_in_usd_parallel else 0 end
        ) as total_origin_percentage_fees_collected_in_usd_parallel,

            {# /* Origin forex fees */ #}
        max(
            case when transactor_side = 'ORIGIN' and fee_type = 'FOREX_FEE'
                then forex_fee_percentage_using_usd_parallel else null end
        ) as forex_fee_percentage_using_usd_parallel,
        max(
            case when transactor_side = 'ORIGIN' and fee_type = 'FOREX_FEE'
                then adjusted_exchange_rate_with_forex_fee else null end
        ) as adjusted_exchange_rate_with_forex_fee,
        max(
            case when transactor_side = 'ORIGIN' and fee_type = 'FOREX_FEE'
                then unadjusted_exchange_rate_without_forex_fee else null end
        ) as unadjusted_exchange_rate_without_forex_fee,
        max(
            case when transactor_side = 'ORIGIN' and fee_type = 'FOREX_FEE' 
                then amount_forex_fee_calculated_from else 0 end
        ) as origin_amount_forex_fee_calculated_from,
        max(
            case when transactor_side = 'ORIGIN' and fee_type = 'FOREX_FEE' 
                then amount_in_usd_forex_fee_calculated_from else 0 end
        ) as origin_amount_in_usd_forex_fee_calculated_from,
        max(
            case when transactor_side = 'ORIGIN' and fee_type = 'FOREX_FEE' 
                then amount_in_usd_parallel_forex_fee_calculated_from else 0 end
        ) as origin_amount_in_usd_parallel_forex_fee_calculated_from,
        sum(
            case when transactor_side = 'ORIGIN' and fee_type = 'FOREX_FEE'
                then fee_amount_collected else 0 end
        ) as total_origin_forex_fees_collected,
        sum(
            case when transactor_side = 'ORIGIN' and fee_type = 'FOREX_FEE'
                then fee_amount_collected_in_usd else 0 end
        ) as total_origin_forex_fees_collected_in_usd,
        sum(
            case when transactor_side = 'ORIGIN' and fee_type = 'FOREX_FEE'
                then fee_amount_collected_in_usd_parallel else 0 end
        ) as total_origin_forex_fees_collected_in_usd_parallel,
        
        {# /* Destination side */ #}
        destination_currency,

            {# /* Destination flat fees */ #}
        sum(
            case when transactor_side = 'DESTINATION' and fee_type = 'FLAT_FEE'
                then fee_amount_collected else 0 end
        ) as total_destination_flat_fees_collected,
        sum(
            case when transactor_side = 'DESTINATION' and fee_type = 'FLAT_FEE'
                then fee_amount_collected_in_usd else 0 end
        ) as total_destination_flat_fees_collected_in_usd,
        sum(
            case when transactor_side = 'DESTINATION' and fee_type = 'FLAT_FEE'
                then fee_amount_collected_in_usd_parallel else 0 end
        ) as total_destination_flat_fees_collected_in_usd_parallel,

            {# /* Destination percentage fees */ #}
        max(
            case when transactor_side = 'DESTINATION' and fee_type = 'PERCENTAGE_FEE' 
                then amount_percentage_fee_calculated_from else 0 end
        ) as destination_amount_percentage_fee_calculated_from,
        max(
            case when transactor_side = 'DESTINATION' and fee_type = 'PERCENTAGE_FEE' 
                then amount_in_usd_percentage_fee_calculated_from else 0 end
        ) as destination_amount_in_usd_percentage_fee_calculated_from,
        max(
            case when transactor_side = 'DESTINATION' and fee_type = 'PERCENTAGE_FEE' 
                then amount_in_usd_parallel_percentage_fee_calculated_from else 0 end
        ) as destination_amount_in_usd_parallel_percentage_fee_calculated_from,
        sum(
            case when transactor_side = 'DESTINATION' and fee_type = 'PERCENTAGE_FEE'
                then fee_amount_collected else 0 end
        ) as total_destination_percentage_fees_collected,
        sum(
            case when transactor_side = 'DESTINATION' and fee_type = 'PERCENTAGE_FEE'
                then fee_amount_collected_in_usd else 0 end
        ) as total_destination_percentage_fees_collected_in_usd,
        sum(
            case when transactor_side = 'DESTINATION' and fee_type = 'PERCENTAGE_FEE'
                then fee_amount_collected_in_usd_parallel else 0 end
        ) as total_destination_percentage_fees_collected_in_usd_parallel

    from {{ref('fee_collections')}} as fee_collections
    group by
        transfer_quote_id,
        transfer_id,
        transaction_currency_pair,
        origin_currency,
        destination_currency
)

select
    transfer_quotes.id as transfer_quote_id,
    transfer_quotes.transfer_id,
    expanded_transfers.journal_id,
    expanded_transfers.transfer_type,
    expanded_transfers.journal_type,
    transfer_quotes.activity_type,
    transfer_quotes.payment_grouping,
    transaction_currency_pair,

    {# /* Origin side */ #}
    expanded_transfers.origin_currency,
    expanded_transfers.origin_rate as origin_rate_to_usd,
    origin_ngn_parallel_rates.rate as origin_parallel_rate,
    iff(
        expanded_transfers.origin_currency = 'NGN',
        1 / origin_parallel_rate,
        origin_rate_to_usd
    ) as origin_inverse_parallel_rate,

        {# /* Origin amount debited */ #}
    expanded_transfers.origin_amount as origin_amount_debited,
    expanded_transfers.origin_amount_in_usd as origin_amount_debited_in_usd,
    origin_amount_debited * origin_inverse_parallel_rate as origin_amount_debited_in_usd_parallel,

        {# /* Origin flat fees */ #}
    total_origin_flat_fees_collected,
    total_origin_flat_fees_collected_in_usd,
    total_origin_flat_fees_collected_in_usd_parallel,

        {# /* Origin percentage fees */ #}
    origin_amount_percentage_fee_calculated_from,
    origin_amount_in_usd_percentage_fee_calculated_from,
    origin_amount_in_usd_parallel_percentage_fee_calculated_from,
    total_origin_percentage_fees_collected,
    total_origin_percentage_fees_collected_in_usd,
    total_origin_percentage_fees_collected_in_usd_parallel,

        {# /* Origin non-forex fees subtotals */ #}
    total_origin_flat_fees_collected + total_origin_percentage_fees_collected 
        as total_origin_non_forex_fees_collected,
    total_origin_flat_fees_collected_in_usd + total_origin_percentage_fees_collected_in_usd 
        as total_origin_non_forex_fees_collected_in_usd,
    total_origin_flat_fees_collected_in_usd_parallel + total_origin_percentage_fees_collected_in_usd_parallel 
        as total_origin_non_forex_fees_collected_in_usd_parallel,

        {# /* Origin amount less non-forex fees subtotals */ #}
    transfer_quotes.origin_amount_before_fees as origin_amount_less_non_forex_fees,
    origin_amount_less_non_forex_fees * origin_rate_to_usd 
        as origin_amount_less_non_forex_fees_in_usd,
    origin_amount_less_non_forex_fees * origin_inverse_parallel_rate 
        as origin_amount_less_non_forex_fees_in_usd_parallel,

        {# /* Origin forex fees */ #}
    expanded_transfers.exchange_rate_fee_percentage,
    expanded_transfers.base_modification_percentage,
    forex_fee_percentage_using_usd_parallel,
    adjusted_exchange_rate_with_forex_fee,
    unadjusted_exchange_rate_without_forex_fee,
    origin_amount_forex_fee_calculated_from,
    origin_amount_in_usd_forex_fee_calculated_from,
    origin_amount_in_usd_parallel_forex_fee_calculated_from,
    total_origin_forex_fees_collected,
    total_origin_forex_fees_collected_in_usd,
    total_origin_forex_fees_collected_in_usd_parallel,

        {# /* Origin total fees: non-forex fees subtotals + forex fees */ #}
    total_origin_non_forex_fees_collected + total_origin_forex_fees_collected
        as total_origin_fee_collected,
    total_origin_non_forex_fees_collected_in_usd + total_origin_forex_fees_collected_in_usd
        as total_origin_fees_collected_in_usd,
    total_origin_non_forex_fees_collected_in_usd_parallel + total_origin_forex_fees_collected_in_usd_parallel
        as total_origin_fees_collected_in_usd_parallel,

        {# /* Origin amount less all fees: (origin amount less non-forex fees) - forex fees */ #}
    origin_amount_less_non_forex_fees - total_origin_forex_fees_collected
        as origin_amount_less_all_fees,
    origin_amount_less_non_forex_fees_in_usd - total_origin_forex_fees_collected_in_usd
        as origin_amount_less_all_fees_in_usd,
    origin_amount_less_non_forex_fees_in_usd_parallel - total_origin_forex_fees_collected_in_usd_parallel
        as origin_amount_less_all_fees_in_usd_parallel,

        {# /* Origin user IDs */ #}
    expanded_transfers.outgoing_user_id,
    transfer_quotes.sender_id,

    {# /* Destination side */ #}
    expanded_transfers.destination_currency,
    expanded_transfers.destination_rate as destination_rate_to_usd,
    destination_ngn_parallel_rates.rate as destination_parallel_rate,
    iff(
        expanded_transfers.destination_currency = 'NGN',
        1 / destination_parallel_rate,
        destination_rate_to_usd
    ) as destination_inverse_parallel_rate,
    
        {# /* Destination amount after forex conversion, but before any destination-side fees are deducted */ #}
    transfer_quotes.destination_amount_before_fees as destination_amount_plus_non_forex_fees,
    destination_amount_plus_non_forex_fees * destination_rate_to_usd 
        as destination_amount_plus_non_forex_fees_in_usd,
    destination_amount_plus_non_forex_fees * destination_inverse_parallel_rate
        as destination_amount_plus_non_forex_fees_in_usd_parallel,

        {# /* Destination flat fees */ #}
    total_destination_flat_fees_collected,
    total_destination_flat_fees_collected_in_usd,
    total_destination_flat_fees_collected_in_usd_parallel,

        {# /* Destination percentage fees */ #}
    destination_amount_percentage_fee_calculated_from,
    destination_amount_in_usd_percentage_fee_calculated_from,
    destination_amount_in_usd_parallel_percentage_fee_calculated_from,
    total_destination_percentage_fees_collected,
    total_destination_percentage_fees_collected_in_usd,
    total_destination_percentage_fees_collected_in_usd_parallel,

        {# /* Destination non-forex fees subtotals */ #}
    total_destination_flat_fees_collected + total_destination_percentage_fees_collected 
        as total_destination_non_forex_fees_collected,
    total_destination_flat_fees_collected_in_usd + total_destination_percentage_fees_collected_in_usd 
        as total_destination_non_forex_fees_collected_in_usd,
    total_destination_flat_fees_collected_in_usd_parallel + total_destination_percentage_fees_collected_in_usd_parallel 
        as total_destination_non_forex_fees_collected_in_usd_parallel,

        {# /* Destination amount credited */ #}
    expanded_transfers.destination_amount as destination_amount_credited,
    expanded_transfers.destination_amount_in_usd as destination_amount_credited_in_usd,
    destination_amount_credited * destination_inverse_parallel_rate as destination_amount_credited_in_usd_parallel,

        {# /* Destination user IDs */ #}
    expanded_transfers.incoming_user_id,
    transfer_quotes.recipient_id,

        {# /* Transaction gains and losses in USD */ #}
    origin_amount_debited_in_usd - destination_amount_credited_in_usd 
        as gains_and_losses_in_usd,
    origin_amount_debited_in_usd_parallel - destination_amount_credited_in_usd_parallel 
        as gains_and_losses_in_usd_parallel,

        {# /* Relevant timestamps */ #}
    transfer_quotes.created_at as transfer_quote_created_at,
    transfer_quotes.updated_at as transfer_quote_updated_at,
    transfer_quotes.valid_until as transfer_quote_valid_until,
    expanded_transfers.hlo_created_at,
    expanded_transfers.hlo_updated_at,

    expanded_transfers.hlo_status,
    expanded_transfers.is_original_transfer_reversed,
    expanded_transfers.is_transfer_reversal,

        {# /* Other columns of transfer quotes */ #}
    transfer_quotes.linked_account_id,
    transfer_quotes.linked_account_type,
    transfer_quotes.is_destination_transfer,
    transfer_quotes.user_segment,
    transfer_quotes.quote_type

from {{ref('transfer_quotes')}} as transfer_quotes

join chipper.dbt_transformations.expanded_transfers as expanded_transfers
    on transfer_quotes.transfer_id = expanded_transfers.transfer_id

left join aggregated_fee_collections
    on transfer_quotes.id = aggregated_fee_collections.transfer_quote_id
    and transfer_quotes.transfer_id = aggregated_fee_collections.transfer_id

left join ngn_parallel_rates as origin_ngn_parallel_rates
    on cast(expanded_transfers.hlo_created_at as date) = origin_ngn_parallel_rates.date
    and expanded_transfers.origin_currency = origin_ngn_parallel_rates.currency

left join ngn_parallel_rates as destination_ngn_parallel_rates
    on cast(expanded_transfers.hlo_created_at as date) = destination_ngn_parallel_rates.date
    and expanded_transfers.destination_currency = destination_ngn_parallel_rates.currency
