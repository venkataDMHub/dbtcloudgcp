{{ config(materialized='table', schema='looker', unique_key='row_number') }}

with contribution_margin as (

{{ dbt_utils.union_relations(
    relations=[
    ref('forex_contribution_margin'),
    ref('non_forex_contribution_margin'),
    ref('cards_contribution_margin'),
    ref('cico_contribution_margin'),
    ref('crypto_contribution_margin'),
    ref('data_bundle_contribution_margin'),
    ref('airtime_contribution_margin'),
    ref('stocks_contribution_margin'),
    ref('network_api_contribution_margin')]
) }}

)

select
    margin_stream,
    revenue_stream,
    transfer_id,
    transaction_settled_date,
    journal_type,
    transfer_type,
    hlo_status,
    corridor,
    external_provider,
    user_id,
    primary_currency,
    user_kyc_tier,
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
    hlo_table_with_id,
    fee_calculation_id,
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
    contribution_margin_in_usd_parallel,
    dense_rank() over (
        order by
            margin_stream,
            revenue_stream,
            hlo_table_with_id,
            transfer_id,
            fee_calculation_id,
            user_id,
            net_revenues_in_usd,
            cogs_in_usd,
            contribution_margin_in_usd
    ) as row_number
    
from contribution_margin
where hlo_table_with_id is not null
{{ dbt_utils.group_by(n=36) }}
