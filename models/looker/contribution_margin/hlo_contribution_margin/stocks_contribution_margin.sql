{{ config(materialized='table',schema='intermediate', unique_key='hlo_table_with_id') }}

{% set tpv_drivewealth_fee_pctg = '0.004' %}
{% set revenue_drivewealth_fee_pctg = '0.4' %}

with
    parallel_rates as (

        select distinct date, rate, currency
        from chipper.utils.ngn_usd_parallel_market_rates

    ),

    stocks as (

        select
            'STOCK' as margin_stream,
            revenue.revenue_stream,
            transfers.hlo_updated_at as transaction_settled_date,

            date_trunc('month', transaction_settled_date) as month,

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

            stocks.fee_amount,

            case
                when stocks.fee_currency = transfers.destination_currency
                then transfers.destination_rate
                when stocks.fee_currency = transfers.origin_currency
                then transfers.origin_rate
                else null
            end as rate_to_usd,

            parallel_rates.rate as parallel_rate,

            fee_calculations.id as fee_calculation_id,

            volume.hlo_table_with_id,
            (zeroifnull(volume.credit_side_unadjusted_volume) + zeroifnull(volume.debit_side_unadjusted_volume)) as tpv,
            volume.unadjusted_transaction_volume_in_usd as tpv_in_usd,
            volume.unadjusted_transaction_volume_in_usd_parallel as tpv_in_usd_parallel,

            revenue.gross_revenues,
            revenue.gross_revenues_in_usd,
            revenue.gross_revenues_in_usd_parallel,
            revenue.net_revenues,
            revenue.net_revenues_in_usd,
            revenue.net_revenues_in_usd_parallel,

            --BEGIN LOGIC FOR MONTHLY COST STRUCTURE

            count(transfers.transfer_id) over (partition by month order by month) as total_monthly_transactions,
            sum(tpv) over (partition by month order by month) as total_monthly_tpv,
            sum(tpv_in_usd) over (partition by month order by month) as total_monthly_tpv_in_usd,
            sum(tpv_in_usd_parallel) over (partition by month order by month) as total_monthly_tpv_in_usd_parallel,
            sum(net_revenues) over (partition by month order by month) as total_monthly_revenue,
            sum(net_revenues_in_usd) over (partition by month order by month) as total_monthly_revenue_in_usd,
            sum(net_revenues_in_usd_parallel) over (partition by month order by month) as total_monthly_revenue_in_usd_parallel,

            total_monthly_tpv * {{ tpv_drivewealth_fee_pctg }} as total_monthly_tpv_cost,
            total_monthly_tpv_in_usd * {{ tpv_drivewealth_fee_pctg }} as total_monthly_tpv_cost_in_usd,
            total_monthly_tpv_in_usd_parallel * {{ tpv_drivewealth_fee_pctg }} as total_monthly_tpv_cost_in_usd_parallel,
            total_monthly_revenue * {{ revenue_drivewealth_fee_pctg }} as total_monthly_revenue_cost,
            total_monthly_revenue_in_usd * {{ revenue_drivewealth_fee_pctg }} as total_monthly_revenue_cost_in_usd,
            total_monthly_revenue_in_usd_parallel * {{ revenue_drivewealth_fee_pctg }} as total_monthly_revenue_cost_in_usd_parallel,
            10000 as total_monthly_fixed_cost_in_usd,

            total_monthly_tpv_cost/ total_monthly_transactions as monthly_tpv_cost_per_transaction,
            total_monthly_tpv_cost_in_usd / total_monthly_transactions as monthly_tpv_cost_per_transaction_in_usd,
            total_monthly_tpv_cost_in_usd_parallel / total_monthly_transactions as monthly_tpv_cost_per_transaction_in_usd_parallel, 

            total_monthly_revenue_cost / total_monthly_transactions as monthly_revenue_cost_per_transaction,
            total_monthly_revenue_cost_in_usd / total_monthly_transactions as monthly_revenue_cost_per_transaction_in_usd,
            total_monthly_revenue_cost_in_usd_parallel / total_monthly_transactions as monthly_revenue_cost_per_transaction_in_usd_parallel, 

            total_monthly_fixed_cost_in_usd / total_monthly_transactions as monthly_fixed_cost_per_transaction_in_usd,

            --END LOGIC FOR MONTHLY COST STRUCTURE
            ifnull(greatest(monthly_tpv_cost_per_transaction, monthly_revenue_cost_per_transaction, monthly_fixed_cost_per_transaction_in_usd),10000) as cogs,
            ifnull(greatest(monthly_tpv_cost_per_transaction_in_usd, monthly_revenue_cost_per_transaction_in_usd, monthly_fixed_cost_per_transaction_in_usd),10000) as cogs_in_usd,
            ifnull(greatest(monthly_tpv_cost_per_transaction_in_usd_parallel, monthly_revenue_cost_per_transaction_in_usd_parallel, monthly_fixed_cost_per_transaction_in_usd),10000) as cogs_in_usd_parallel,

            (ifnull(net_revenues_in_usd,0) - cogs_in_usd) as gross_margin_in_usd,
            (ifnull(net_revenues_in_usd_parallel,0) - cogs_in_usd_parallel) as gross_margin_in_usd_parallel,

            gross_margin_in_usd as contribution_margin_in_usd,
            gross_margin_in_usd_parallel as contribution_margin_in_usd_parallel

        from chipper.{{ var('core_public') }}.stock_trades as stocks

        left join
            chipper.dbt_transformations.user_demographic_features as users
            on stocks.user_id = users.user_id

        inner join
            chipper.dbt_transformations.expanded_transfers as transfers
            on stocks.transfer_id = transfers.transfer_id

        inner join
            chipper.dbt_transformations.transaction_details as details
            on transfers.transfer_id = details.transfer_id
        
        left join
            chipper.dbt_transformations.aggregated_transaction_volume as volume
            on concat_ws('-', transfers.hlo_table, transfers.hlo_id) = volume.hlo_table_with_id
        
        left join
            chipper.dbt_transformations_looker.revenue_details as revenue
            on transfers.transfer_id = revenue.transfer_id
            and revenue.revenue_stream = 'STOCK_TRADING_FEES'

        left join
            parallel_rates
            on cast(transfers.hlo_created_at as date) = cast(parallel_rates.date as date)
            and transfers.origin_currency = parallel_rates.currency

        left join  
            chipper.dbt_transformations_looker.whales_monthly as whales
            on whales.monetized_user_id = stocks.user_id
            and whales.settled_month = date_trunc('month',transfers.hlo_updated_at)
            and whales.revenue_stream = margin_stream

        left join
            chipper.{{ var("core_public") }}.transfer_quotes as transfer_quotes
            on transfers.transfer_id = transfer_quotes.transfer_id

        left join
            chipper.{{ var("core_public") }}.fee_calculations as fee_calculations
            on transfer_quotes.id = fee_calculations.transfer_quote_id

        where stocks.fee_amount <> '0'
        and details.external_provider_transaction_id is not null
        and users.user_id not in ({{internal_users()}})
        and volume.hlo_table_with_id is not null
    )

select
    margin_stream,
    revenue_stream,
    transaction_settled_date,
    transfer_id,
    transfer_type,
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
    origin_currency,
    origin_amount,
    origin_rate,
    origin_amount_in_usd,
    destination_currency,
    destination_amount,
    destination_rate,
    destination_amount_in_usd,
    rate_to_usd as rate,
    parallel_rate,
    fee_amount as stock_fee_amount,
    total_monthly_tpv_in_usd,
    total_monthly_tpv_in_usd_parallel,
    total_monthly_tpv_cost_in_usd,
    total_monthly_tpv_cost_in_usd_parallel,
    total_monthly_revenue_in_usd,
    total_monthly_revenue_in_usd_parallel,
    total_monthly_revenue_cost_in_usd,
    total_monthly_revenue_cost_in_usd_parallel,
    total_monthly_fixed_cost_in_usd,
    total_monthly_transactions,
    monthly_tpv_cost_per_transaction_in_usd,
    monthly_tpv_cost_per_transaction_in_usd_parallel,
    monthly_revenue_cost_per_transaction_in_usd,
    monthly_revenue_cost_per_transaction_in_usd_parallel,
    monthly_fixed_cost_per_transaction_in_usd,
    hlo_table_with_id,
    tpv_in_usd,
    tpv_in_usd_parallel,
    net_revenues_in_usd,
    net_revenues_in_usd_parallel,
    cogs,
    cogs_in_usd,
    cogs_in_usd_parallel,
    gross_margin_in_usd,
    gross_margin_in_usd_parallel,
    0 as operating_expenses_in_usd,
    0 as operating_expenses_in_usd_parallel,
    contribution_margin_in_usd,
    contribution_margin_in_usd_parallel
from stocks
