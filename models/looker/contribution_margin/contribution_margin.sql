{{ config(materialized='table', schema='looker', unique_key='hlo_table_with_id') }}

with aggregated_contribution_margin_details as (
    select
        hlo_table_with_id,
        max(margin_stream) as margin_stream,
        max(transaction_settled_date) as transaction_settled_date,
        max(journal_type) as journal_type,
        max(transfer_type) as transfer_type,
        max(hlo_status) as hlo_status,
        max(corridor) as corridor,
        max(external_provider) as external_provider,
        max(user_id) as user_id,
        max(primary_currency) as primary_currency,
        max(user_kyc_tier) as user_kyc_tier,
        max(user_acquisition_source) as user_acquisition_source,
        max(user_is_internal) as user_is_internal,
        max(user_is_deleted) as user_is_deleted,
        max(user_is_admin) as user_is_admin,
        max(user_is_business) as user_is_business,
        max(user_is_valid_user) as user_is_valid_user,
        max(user_is_blocked_by_flag) as user_is_blocked_by_flag,
        max(user_has_risk_flag) as user_has_risk_flag,
        max(is_whale) as is_whale,
        max(is_parallel_whale) as is_parallel_whale,
        min(user_id) as margin_second_user_id,

    --FOREX SPECIFIC REVENUES--
        max(case when revenue_stream = 'FOREX_FEES' then tpv_in_usd else 0 end) as forex_tpv_in_usd,
        max(case when revenue_stream = 'FOREX_FEES' then tpv_in_usd_parallel else 0 end) as forex_tpv_in_usd_parallel,
        sum(case when revenue_stream = 'FOREX_FEES' then net_revenues_in_usd else 0 end) as forex_net_revenues_in_usd,
        sum(case when revenue_stream = 'FOREX_FEES' then net_revenues_in_usd_parallel else 0 end) as forex_net_revenues_in_usd_parallel,
        sum(case when revenue_stream = 'FOREX_FEES' then cogs_in_usd else 0 end) as forex_cogs_in_usd,
        sum(case when revenue_stream = 'FOREX_FEES' then cogs_in_usd_parallel else 0 end) as forex_cogs_in_usd_parallel,
        sum(case when revenue_stream = 'FOREX_FEES' then gross_margin_in_usd else 0 end) as forex_gross_margin_in_usd,
        sum(case when revenue_stream = 'FOREX_FEES' then gross_margin_in_usd_parallel else 0 end) as forex_gross_margin_in_usd_parallel,
        sum(case when revenue_stream = 'FOREX_FEES' then operating_expenses_in_usd else 0 end) as forex_operating_expenses_in_usd,
        sum(case when revenue_stream = 'FOREX_FEES' then operating_expenses_in_usd_parallel else 0 end) as forex_operating_expenses_in_usd_parallel,
        sum(case when revenue_stream = 'FOREX_FEES' then contribution_margin_in_usd else 0 end) as forex_contribution_margin_in_usd,
        sum(case when revenue_stream = 'FOREX_FEES' then contribution_margin_in_usd_parallel else 0 end) as forex_contribution_margin_in_usd_parallel,
    --END FOREX SPECIFIC REVENUES-- 

    --PRODUCT SPECIFIC REVENUES--
        max(case when revenue_stream != 'FOREX_FEES' then tpv_in_usd else 0 end) as product_tpv_in_usd,
        max(case when revenue_stream != 'FOREX_FEES' then tpv_in_usd_parallel else 0 end) as product_tpv_in_usd_parallel,
        sum(case when revenue_stream != 'FOREX_FEES' then net_revenues_in_usd else 0 end) as product_net_revenues_in_usd,
        sum(case when revenue_stream != 'FOREX_FEES' then net_revenues_in_usd_parallel else 0 end) as product_net_revenues_in_usd_parallel,
        sum(case when revenue_stream != 'FOREX_FEES' then cogs_in_usd else 0 end) as product_cogs_in_usd,
        sum(case when revenue_stream != 'FOREX_FEES' then cogs_in_usd_parallel else 0 end) as product_cogs_in_usd_parallel,
        sum(case when revenue_stream != 'FOREX_FEES' then gross_margin_in_usd else 0 end) as product_gross_margin_in_usd,
        sum(case when revenue_stream != 'FOREX_FEES' then gross_margin_in_usd_parallel else 0 end) as product_gross_margin_in_usd_parallel,
        sum(case when revenue_stream != 'FOREX_FEES' then operating_expenses_in_usd else 0 end) as product_operating_expenses_in_usd,
        sum(case when revenue_stream != 'FOREX_FEES' then operating_expenses_in_usd_parallel else 0 end) as product_operating_expenses_in_usd_parallel,
        sum(case when revenue_stream != 'FOREX_FEES' then contribution_margin_in_usd else 0 end) as product_contribution_margin_in_usd,
        sum(case when revenue_stream != 'FOREX_FEES' then contribution_margin_in_usd_parallel else 0 end) as product_contribution_margin_in_usd_parallel,
    --END PRODUCT SPECIFIC REVENUES--

    --TOTAL REVENUES--
        max(tpv_in_usd) as tpv_in_usd,
        max(tpv_in_usd_parallel) as tpv_in_usd_parallel,
        sum(net_revenues_in_usd) as net_revenues_in_usd,
        sum(net_revenues_in_usd_parallel) as net_revenues_in_usd_parallel,
        sum(cogs_in_usd) as cogs_in_usd,
        sum(cogs_in_usd_parallel) as cogs_in_usd_parallel,
        sum(gross_margin_in_usd) as gross_margin_in_usd,
        sum(gross_margin_in_usd_parallel) as gross_margin_in_usd_parallel,
        sum(operating_expenses_in_usd) as operating_expenses_in_usd,
        sum(operating_expenses_in_usd_parallel) as operating_expenses_in_usd_parallel,
        sum(contribution_margin_in_usd) as contribution_margin_in_usd,
        sum(contribution_margin_in_usd_parallel) as contribution_margin_in_usd_parallel
    --END TOTAL REVENUES--
        
    from {{ ref("contribution_margin_details") }}
    where hlo_table_with_id is not null
    {{ dbt_utils.group_by(n=1) }}
)

select 
    contribution_margin.*
    
from aggregated_contribution_margin_details as contribution_margin

left join chipper.dbt_transformations.expanded_users as margin_first_users
    on contribution_margin.user_id = margin_first_users.user_id

left join chipper.dbt_transformations.expanded_users as margin_second_users
    on contribution_margin.margin_second_user_id = margin_second_users.user_id
