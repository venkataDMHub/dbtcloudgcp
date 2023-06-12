{{ config(materialized='table', schema='intermediate', unique_key='hlo_table_with_id') }}

with
    parallel_rates as (

        select distinct date, rate, currency
        from chipper.utils.ngn_usd_parallel_market_rates

    ),

    crypto as (

        select
            'CRYPTO' as margin_stream,
            'CRYPTO_SALES' as revenue_stream,

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

            case
                when transfers.transfer_type = 'ASSET_TRADES_BUY_SETTLED'
                then transfers.destination_currency
                when transfers.transfer_type = 'ASSET_TRADES_SELL_SETTLED'
                then transfers.origin_currency
                else null
            end as cryptocurrency,
            case
                when transfers.transfer_type = ('ASSET_TRADES_BUY_SETTLED')
                then transfers.origin_rate
                when transfers.transfer_type = ('ASSET_TRADES_SELL_SETTLED')
                then transfers.destination_rate
            end as rate,
            case
                when transfers.transfer_type = ('ASSET_TRADES_BUY_SETTLED')
                then origin_ngn_parallel_rates.rate
                when transfers.transfer_type = ('ASSET_TRADES_SELL_SETTLED')
                then destination_ngn_parallel_rates.rate
            end as parallel_rate,

            volume.hlo_table_with_id,
            volume.adjusted_transaction_volume_in_usd as tpv_in_usd,

            --TPV for crypto using the parallel rate will be the same as TPV using official rate because we are using the CRYPTOCURRENCY amount instead of the FIAT amount
            volume.adjusted_transaction_volume_in_usd_parallel as tpv_in_usd_parallel,

            transfers.origin_amount_in_usd as net_revenues_in_usd,

            case
                when
                    transfers.transfer_type = 'ASSET_TRADES_SELL_SETTLED'
                    or (
                        transfers.transfer_type = 'ASSET_TRADES_BUY_SETTLED'
                        and transfers.origin_currency != 'NGN'
                    )
                then transfers.origin_amount_in_usd
                when
                    transfers.transfer_type = 'ASSET_TRADES_BUY_SETTLED'
                    and transfers.origin_currency = 'NGN'
                    and origin_ngn_parallel_rates.rate is not null
                then transfers.origin_amount / origin_ngn_parallel_rates.rate
                else null
            end as net_revenues_in_usd_parallel,

            transfers.destination_amount_in_usd as cogs_in_usd,

            case
                when
                    transfers.transfer_type = 'ASSET_TRADES_BUY_SETTLED'
                    or (
                        transfers.transfer_type = 'ASSET_TRADES_SELL_SETTLED'
                        and transfers.destination_currency != 'NGN'
                    )
                then transfers.destination_amount_in_usd
                when
                    transfers.transfer_type = 'ASSET_TRADES_SELL_SETTLED'
                    and transfers.destination_currency = 'NGN'
                    and destination_ngn_parallel_rates.rate is not null
                then transfers.destination_amount / destination_ngn_parallel_rates.rate
                else null
            end as cogs_in_usd_parallel,

            (transfers.origin_amount_in_usd - transfers.destination_amount_in_usd) as gross_margin_in_usd,
            (net_revenues_in_usd_parallel - cogs_in_usd_parallel) as gross_margin_in_usd_parallel,
            0 as operating_expenses_in_usd,
            0 as operating_expenses_in_usd_parallel,
            gross_margin_in_usd as contribution_margin_in_usd,
            gross_margin_in_usd_parallel as contribution_margin_in_usd_parallel

        from chipper.dbt_transformations.expanded_transfers as transfers

        inner join
            chipper.dbt_transformations.transaction_details as details
            on transfers.transfer_id = details.transfer_id

        left join
            chipper.dbt_transformations.aggregated_transaction_volume as volume
            on concat_ws('-', transfers.hlo_table, transfers.hlo_id) = volume.hlo_table_with_id

        left join
            chipper.dbt_transformations.user_demographic_features as users
            on transfers.incoming_user_id = users.user_id

        left join
            parallel_rates as origin_ngn_parallel_rates
            on cast(transfers.hlo_created_at as date) = origin_ngn_parallel_rates.date
            and transfers.origin_currency = origin_ngn_parallel_rates.currency

        left join
            parallel_rates as destination_ngn_parallel_rates
            on cast(transfers.hlo_created_at as date)
            = destination_ngn_parallel_rates.date
            and transfers.destination_currency = destination_ngn_parallel_rates.currency

        left join
            chipper.dbt_transformations_looker.whales_monthly as whales
            on whales.monetized_user_id = transfers.incoming_user_id
            and whales.settled_month = date_trunc('month', transfers.hlo_updated_at)
            and whales.revenue_stream = margin_stream

        where
            transfers.transfer_type
            in ('ASSET_TRADES_BUY_SETTLED', 'ASSET_TRADES_SELL_SETTLED')
            and transfers.is_original_transfer_reversed = 'FALSE'
            and transfers.hlo_status in ('SETTLED', 'COMPLETED')
            and users.user_id not in ({{internal_users()}})
            and volume.hlo_table_with_id is not null
    )

select *
from crypto
