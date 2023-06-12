{{ config(materialized="table", schema="intermediate", unique_key="hlo_table_with_id") }}

with
    parallel_rates as (

        select distinct date, rate, currency
        from chipper.utils.ngn_usd_parallel_market_rates

    ),

    intouchpay_withdrawals as (

        select
            'CASH_OUT' as margin_stream,
            transfers.*,
            volume.hlo_table_with_id,
            volume.unadjusted_transaction_volume_in_usd as tpv_in_usd,
            volume.unadjusted_transaction_volume_in_usd_parallel as tpv_in_usd_parallel,
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
            details.transaction_details,
            details.external_provider,
            null as parallel_rate,
            '200' as fee,
            fee * destination_rate as fee_in_usd

        from chipper.dbt_transformations.expanded_transfers as transfers

        inner join
            chipper.dbt_transformations.transaction_details as details
            on transfers.transfer_id = details.transfer_id

        left join
            chipper.dbt_transformations.aggregated_transaction_volume as volume
            on concat_ws('-', transfers.hlo_table, transfers.hlo_id) = volume.hlo_table_with_id

        left join
            chipper.dbt_transformations.user_demographic_features as users
            on transfers.outgoing_user_id = users.user_id

        left join
            parallel_rates as origin_ngn_parallel_rates
            on cast(transfers.hlo_created_at as date) = origin_ngn_parallel_rates.date
            and transfers.origin_currency = origin_ngn_parallel_rates.currency

        where
            details.external_provider_transaction_id is not null
            and journal_type = 'WITHDRAWAL'
            and details.external_provider = 'INTOUCHPAY'
            and transfers.destination_currency = 'RWF'
            and users.user_id not in ({{internal_users()}})
            and volume.hlo_table_with_id is not null
    )

select
    margin_stream,
    hlo_updated_at as transaction_settled_date,
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
    origin_currency,
    origin_amount,
    origin_rate,
    origin_amount_in_usd,
    destination_currency,
    destination_amount,
    destination_rate,
    destination_amount_in_usd,
    hlo_table_with_id,
    tpv_in_usd,
    tpv_in_usd_parallel,
    destination_currency as fee_currency,
    destination_rate as rate,
    parallel_rate,
    fee,
    fee_in_usd,
    fee_in_usd as fee_in_usd_parallel
from intouchpay_withdrawals
