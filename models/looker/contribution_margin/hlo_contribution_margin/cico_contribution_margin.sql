{{ config(materialized="table", schema="intermediate", unique_key="hlo_table_with_id") }}

with
    all_withdrawals_and_deposits as (

    {{ dbt_utils.union_relations(
        relations=[
        ref('pegasus_withdrawals'),
        ref('rave_deposits'),
        ref('rave_withdrawals'),
        ref('wiztransact_rw_withdrawals'),
        ref('airtel_ug_withdrawals'),
        ref('intouchpay_withdrawals'),
        ref('ninepsb_withdrawals'),
        ref('cellulant_ug_withdrawals'),
        ref('airtel_ug_deposits'),
        ref('intouchpay_deposits'),
        ref('ninepsb_deposits'),
        ref('pegasus_deposits')
        ]
    ) }}

    )

select
    margin_stream,
    margin_stream as revenue_stream,
    transaction_settled_date,
    transfer_id,
    transfer_type,
    hlo_status,
    corridor,
    transfer_status,
    transfer_created_at,
    transfer_updated_at,
    journal_type,
    user_id,
    external_provider,
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
    null as is_whale,
    null as is_parallel_whale,
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
    origin_currency as fee_currency,
    origin_rate as rate,
    parallel_rate,
    fee,
    fee_in_usd,
    fee_in_usd_parallel,
    0 as net_revenues_in_usd,
    0 as net_revenues_in_usd_parallel,
    fee_in_usd as cogs_in_usd,
    fee_in_usd_parallel as cogs_in_usd_parallel,
    net_revenues_in_usd - cogs_in_usd as gross_margin_in_usd,
    net_revenues_in_usd_parallel - cogs_in_usd_parallel as gross_margin_in_usd_parallel,
    0 as operating_expenses_in_usd,
    0 as operating_expenses_in_usd_parallel,
    gross_margin_in_usd as contribution_margin_in_usd,
    gross_margin_in_usd_parallel as contribution_margin_in_usd_parallel

from all_withdrawals_and_deposits as transactions
