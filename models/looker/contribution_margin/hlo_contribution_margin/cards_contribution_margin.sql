{{ config(materialized='table',schema='intermediate', unique_key='hlo_table_with_id') }}

with
    final_cards as (

select
    margin_stream,
    revenue_stream,
    transaction_settled_date,
    transfer_id,
    hlo_status,
    corridor,
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
    journal_type,
    transfer_type,
    rate,
    parallel_rate,
    hlo_table_with_id,
    tpv_in_usd,
    tpv_in_usd_parallel,
    net_revenues_in_usd,
    net_revenues_in_usd_parallel,
    cogs_in_usd,
    cogs_in_usd_parallel,
    gross_margin_in_usd,
    gross_margin_in_usd_parallel,
    operating_expenses_in_usd,
    operating_expenses_in_usd_parallel,
    contribution_margin_in_usd,
    contribution_margin_in_usd_parallel
from {{ ref("card_transactions_contribution_margin") }}

union all

select
    margin_stream,
    revenue_stream,
    transaction_settled_date,
    transfer_id,
    hlo_status,
    corridor,
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
    journal_type,
    transfer_type,
    rate,
    parallel_rate,
    hlo_table_with_id,
    tpv_in_usd,
    tpv_in_usd_parallel,
    net_revenues_in_usd,
    net_revenues_in_usd_parallel,
    cogs_in_usd,
    cogs_in_usd_parallel,
    gross_margin_in_usd,
    gross_margin_in_usd_parallel,
    operating_expenses_in_usd,
    operating_expenses_in_usd_parallel,
    contribution_margin_in_usd,
    contribution_margin_in_usd_parallel
from {{ ref("card_payments_contribution_margin") }}

)

select * from final_cards
