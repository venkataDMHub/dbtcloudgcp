{{ config(materialized="table", schema="intermediate", unique_key="hlo_table_with_id") }}

with
    parallel_rates as (

        select distinct date, rate, currency
        from chipper.utils.ngn_usd_parallel_market_rates

    ),

    airtime as (

        select
            'AIRTIME' as margin_stream,
            'AIRTIME_SALES' as revenue_stream,
            transfers.hlo_updated_at as transaction_settled_date,

            transfers.transfer_id,
            transfers.transfer_type,
            transfers.hlo_status,
            transfers.transfer_status,
            transfers.transfer_created_at,
            transfers.transfer_updated_at,
            transfers.journal_type,
            transfers.outgoing_user_id,
            transfers.origin_currency,
            transfers.origin_amount,
            transfers.origin_rate,
            transfers.origin_amount_in_usd,
            transfers.incoming_user_id,
            transfers.destination_currency,
            transfers.destination_amount,
            transfers.destination_rate,
            transfers.destination_amount_in_usd,
            transfers.corridor,

            details.external_provider,

            users.user_id,
            users.primary_currency,
            users.kyc_tier as user_kyc_tier,
            users.acquisition_date as user_acquisition_date,
            users.acquisition_source as user_acquisition_source,
            users.is_internal as user_is_internal,
            users.is_deleted as user_is_deleted,
            users.is_admin as user_is_admin,
            users.is_business as user_is_business,
            users.is_valid_user as user_is_valid_user,
            users.is_blocked_by_flag as user_is_blocked_by_flag,
            users.has_risk_flag as user_has_risk_flag,
            whales.is_whale,
            whales.is_parallel_whale,

            airtime.airtime_provider,
            airtime.currency as airtime_currency,
            airtime.phone_carrier,
            airtime.receive_value,
            airtime.discount_percentage,
            airtime.commission_percentage,
            airtime.send_value,

            airtime.receive_value * (1 - (airtime.discount_percentage / 100)) as discounted_price,
            airtime.receive_value * (1 - (airtime.commission_percentage / 100)) as commission_cost,

            case
                when airtime.currency = transfers.destination_currency
                then transfers.destination_rate
                when airtime.currency = transfers.origin_currency
                then transfers.origin_rate
                else null
            end as rate_to_usd,

            parallel_rates.rate as parallel_rate,

            volume.hlo_table_with_id,
            volume.adjusted_transaction_volume_in_usd as tpv_in_usd,
            volume.adjusted_transaction_volume_in_usd_parallel as tpv_in_usd_parallel,

            revenue.gross_revenues,
            revenue.gross_revenues_in_usd,
            revenue.gross_revenues_in_usd_parallel,
            revenue.net_revenues,
            revenue.net_revenues_in_usd,
            revenue.net_revenues_in_usd_parallel,

            commission_cost as cogs,

            commission_cost * rate_to_usd as cogs_in_usd,
            
            case 
                when (airtime.airtime_provider in ('DING', 'XTRANSBITS') and airtime.currency = 'NGN') 
                then cogs_in_usd
                when (airtime.airtime_provider not in ('DING', 'XTRANSBITS') and airtime.currency = 'NGN') and parallel_rates.rate is not null 
                then cogs / parallel_rates.rate
                else cogs_in_usd
            end as cogs_in_usd_parallel,

            net_revenues_in_usd - cogs_in_usd as gross_margin_in_usd,
            net_revenues_in_usd_parallel - cogs_in_usd_parallel as gross_margin_in_usd_parallel,

            0 as operating_expenses_in_usd,
            0 as operating_expenses_in_usd_parallel,

            gross_margin_in_usd as contribution_margin_in_usd,
            gross_margin_in_usd_parallel as contribution_margin_in_usd_parallel

        from chipper.{{ var("core_public") }}.airtime_purchases as airtime

        left join
            chipper.dbt_transformations.user_demographic_features as users
            on airtime.user_id = users.user_id

        inner join
            chipper.dbt_transformations.expanded_transfers as transfers
            on airtime.transfer_id = transfers.transfer_id

        inner join
            chipper.dbt_transformations.transaction_details as details
            on transfers.transfer_id = details.transfer_id

        left join
            chipper.dbt_transformations_looker.revenue_details as revenue
            on transfers.transfer_id = revenue.transfer_id
            and revenue.revenue_stream = 'AIRTIME_SALES'
        
        left join
            chipper.dbt_transformations.aggregated_transaction_volume as volume
            on concat_ws('-', transfers.hlo_table, transfers.hlo_id) = volume.hlo_table_with_id

        left join
            parallel_rates
            on cast(airtime.created_at as date) = cast(parallel_rates.date as date)
            and airtime.currency = parallel_rates.currency

        left join
            chipper.dbt_transformations_looker.whales_monthly as whales
            on whales.monetized_user_id = airtime.user_id
            and whales.settled_month = date_trunc('month', transfers.hlo_updated_at)
            and whales.revenue_stream = margin_stream

        where
            transfers.hlo_status in ('SETTLED', 'COMPLETED')
            and transfers.is_original_transfer_reversed = 'FALSE'
            and users.user_id not in ({{internal_users()}})
            and volume.hlo_table_with_id is not null
    )

    
select *
from airtime
