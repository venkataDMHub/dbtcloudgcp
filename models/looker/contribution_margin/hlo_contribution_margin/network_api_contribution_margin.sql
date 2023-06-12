{{ config(materialized='table',schema='intermediate', unique_key='hlo_table_with_id') }}

with

    parallel_rates as (

        select distinct date, rate, currency
        from chipper.utils.ngn_usd_parallel_market_rates

    ),

    network_api as (

         select
            'NETWORK_API' as margin_stream,
            revenues.revenue_stream as revenue_stream,
            revenues.transaction_updated_at as transaction_settled_date,
            
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

            case 
                when transfers.transfer_type = 'NETWORK_API_C2B_SETTLED'
                then transfers.incoming_user_id
                when transfers.transfer_type = 'NETWORK_API_B2C_SETTLED'
                then transfers.outgoing_user_id
            end as merchant_user_id,

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

            rate_to_usd as rate,

            revenues.ngn_parallel_rate as parallel_rate,

            fee_calculations.id as fee_calculation_id,

            volume.hlo_table_with_id,
            volume.adjusted_transaction_volume_in_usd as tpv_in_usd,
            volume.adjusted_transaction_volume_in_usd_parallel as tpv_in_usd_parallel,

            revenues.net_revenues,
            revenues.net_revenues_in_usd as net_revenues_in_usd,
            revenues.net_revenues_in_usd_parallel as net_revenues_in_usd_parallel,

            0 as cogs_in_usd,
            0 as cogs_in_usd_parallel,

            net_revenues_in_usd - cogs_in_usd as gross_margin_in_usd,
            net_revenues_in_usd_parallel - cogs_in_usd_parallel as gross_margin_in_usd_parallel,

            0 as operating_expenses_in_usd,
            0 as operating_expenses_in_usd_parallel,

            gross_margin_in_usd as contribution_margin_in_usd,
            gross_margin_in_usd_parallel as contribution_margin_in_usd_parallel

        from chipper.dbt_transformations.expanded_transfers as transfers

        inner join
            chipper.dbt_transformations_looker.revenue_details as revenues
            on transfers.transfer_id = revenues.transfer_id

        left join
            chipper.dbt_transformations.user_demographic_features as users
            on revenues.monetized_user_id = users.user_id

        inner join
            chipper.dbt_transformations.transaction_details as details
            on transfers.transfer_id = details.transfer_id

        left join
            chipper.dbt_transformations.aggregated_transaction_volume as volume
            on concat_ws('-', transfers.hlo_table, transfers.hlo_id) = volume.hlo_table_with_id

        left join
            parallel_rates as parallel_rates
            on cast(transfers.hlo_created_at as date) = parallel_rates.date
            and transfers.origin_currency = parallel_rates.currency

        left join  
            chipper.dbt_transformations_looker.whales_monthly as whales
            on whales.monetized_user_id = revenues.monetized_user_id
            and whales.settled_month = date_trunc('month',transfers.hlo_updated_at)
            and whales.revenue_stream = margin_stream

        left join
            chipper.{{ var("core_public") }}.transfer_quotes as transfer_quotes
            on transfers.transfer_id = transfer_quotes.transfer_id

        left join
            chipper.{{ var("core_public") }}.fee_calculations as fee_calculations
            on transfer_quotes.id = fee_calculations.transfer_quote_id

        where revenues.revenue_stream = 'NETWORK_API_PERCENTAGE_FEES'
        and transfers.hlo_status in ('SETTLED','COMPLETED')
        and transfers.is_original_transfer_reversed = 'FALSE'
        and users.user_id not in ({{internal_users()}})
        and volume.hlo_table_with_id is not null
    )
    
    select *
    from network_api
