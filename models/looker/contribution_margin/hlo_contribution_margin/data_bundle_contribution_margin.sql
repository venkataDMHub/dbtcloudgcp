{{ config(materialized='table',schema='intermediate', unique_key='hlo_table_with_id') }}

with
    parallel_rates as (

        select distinct date, rate, currency
        from chipper.utils.ngn_usd_parallel_market_rates

    ),

    commission_rates as (

        select distinct start_date, end_date, data_provider, currency, carrier, commission_rate, discount_rate
        from chipper.utils.data_purchases_commission_rates

    ),

    data_bundle as (

        select
            'DATA_BUNDLE' as margin_stream,
            'DATA_BUNDLE_SALES' as revenue_stream,
            transfers.hlo_updated_at as transaction_settled_date,

            transfers.transfer_id,
            transfers.transfer_type,
            transfers.hlo_status,
            transfers.transfer_status,
            transfers.transfer_created_at,
            transfers.transfer_updated_at,
            transfers.journal_type,
            transfers.origin_currency,
            transfers.origin_amount,
            transfers.origin_rate,
            transfers.origin_amount_in_usd,
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

            parallel_rates.rate as parallel_rate,

            data_purchases.amount,
            commission_rates.commission_rate,
            data_purchases.amount as gross_revenue,
            gross_revenue as net_revenue,
            net_revenue * (1 - commission_rates.commission_rate) as cogs,

            net_revenue - cogs as gross_margin,

            case
                when data_purchases.currency = transfers.destination_currency
                then destination_rate
                when data_purchases.currency = transfers.origin_currency
                then origin_rate
                else null
            end as rate_to_usd,

            volume.hlo_table_with_id,
            volume.adjusted_transaction_volume_in_usd as tpv_in_usd,
            volume.adjusted_transaction_volume_in_usd_parallel as tpv_in_usd_parallel,            

            gross_revenue * rate_to_usd as gross_revenues_in_usd,
            net_revenue * rate_to_usd as net_revenues_in_usd,
            cogs * rate_to_usd as cogs_in_usd,
            gross_margin * rate_to_usd as gross_margin_in_usd,

            case
                when data_purchases.currency != 'NGN' 
                then gross_revenues_in_usd
                when data_purchases.currency = 'NGN' AND parallel_rates.rate IS NOT NULL 
                then gross_revenue / parallel_rates.rate
                else NULL
            end as gross_revenues_in_usd_parallel,

            case
                when data_purchases.currency != 'NGN' 
                then net_revenues_in_usd
                when data_purchases.currency = 'NGN' AND parallel_rates.rate IS NOT NULL 
                then net_revenue / parallel_rates.rate
                else NULL
            end as net_revenues_in_usd_parallel,

            case
                when data_purchases.currency != 'NGN' 
                then cogs_in_usd
                when data_purchases.currency = 'NGN' AND parallel_rates.rate IS NOT NULL 
                then cogs / parallel_rates.rate
                else NULL
            end as cogs_in_usd_parallel,

            case
                when data_purchases.currency != 'NGN' 
                then gross_margin_in_usd
                when data_purchases.currency = 'NGN' AND parallel_rates.rate IS NOT NULL 
                then gross_margin / parallel_rates.rate
                else NULL
            end as gross_margin_in_usd_parallel

        from chipper.{{ var("core_public") }}.data_purchases as data_purchases

        left join
            chipper.dbt_transformations.user_demographic_features as users
            on data_purchases.user_id = users.user_id

        inner join
            chipper.dbt_transformations.expanded_transfers as transfers
            on data_purchases.transfer_id = transfers.transfer_id

        inner join
            chipper.dbt_transformations.transaction_details as details
            on transfers.transfer_id = details.transfer_id

        left join
            chipper.dbt_transformations.aggregated_transaction_volume as volume
            on concat_ws('-', transfers.hlo_table, transfers.hlo_id) = volume.hlo_table_with_id

        left join
            commission_rates
            on cast(data_purchases.created_at as date) between cast(commission_rates.start_date as date) and cast(commission_rates.end_date as date)
            and data_purchases.currency = commission_rates.currency
            and data_purchases.carrier = commission_rates.carrier
            and data_purchases.data_provider = commission_rates.data_provider

        left join
            parallel_rates
            on cast(data_purchases.created_at as date) = cast(parallel_rates.date as date)
            and data_purchases.currency = parallel_rates.currency

        left join  
            chipper.dbt_transformations_looker.whales_monthly as whales
            on whales.monetized_user_id = data_purchases.user_id
            and whales.settled_month = date_trunc('month',transfers.hlo_updated_at)
            and whales.revenue_stream = margin_stream

        where data_purchases.status = 'COMPLETED'
        and data_purchases.reverse_transfer_id is null
        and users.user_id not in ({{internal_users()}})
        and volume.hlo_table_with_id is not null
    )

select
    margin_stream,
    transaction_settled_date,
    transfer_id,
    hlo_status,
    corridor,
    transfer_status,
    transfer_created_at,
    transfer_updated_at,
    journal_type,
    external_provider,
    user_id,
    primary_currency,
    user_kyc_tier,
    user_acquisition_date,
    user_acquisition_source,
    user_is_internal,
    user_is_deleted,
    user_is_admin,
    user_is_business,
    user_is_valid_user,
    user_is_blocked_by_flag,
    user_has_risk_flag,
    is_whale,
    is_parallel_whale,
    transfer_type,
    rate_to_usd as rate,
    parallel_rate,
    amount,
    commission_rate,
    hlo_table_with_id,
    tpv_in_usd,
    tpv_in_usd_parallel,
    net_revenues_in_usd as net_revenues_in_usd,
    net_revenues_in_usd_parallel as net_revenues_in_usd_parallel,
    cogs_in_usd as cogs_in_usd,
    cogs_in_usd_parallel as cogs_in_usd_parallel,
    gross_margin_in_usd as gross_margin_in_usd,
    gross_margin_in_usd_parallel as gross_margin_in_usd_parallel,
    0 as operating_expenses_in_usd, --placeholder
    0 as operating_expenses_in_usd_parallel, --placeholder
    0 as contribution_margin_in_usd, --placeholder
    0 as contribution_margin_in_usd_parallel --placeholder
from data_bundle
