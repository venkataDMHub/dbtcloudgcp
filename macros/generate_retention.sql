{% macro generate_retention(date_granularity) %}

{# Dictionary that maps date granularity to dim_dates timeframe to days_joined_since to qualify for activation #}
    {% set date_mapping= {'Month' : ['FIRST_DAY_OF_MONTH', 30], 
                          'Week' : ['FIRST_DAY_OF_WEEK', 7],
                          'Quarter': ['FIRST_DAY_OF_QUARTER', 90]
                            }
    %}

{% set query %}

    with dim_dates as (

    Select distinct 
        dd.{{date_mapping[date_granularity][0]}} as date
    from {{ref('dim_dates')}}  dd
    where dd.{{date_mapping[date_granularity][0]}} <= current_date

    ), cm as (

        Select user_id, 
                sum(contribution_margin_in_usd) as cm_in_usd, 
                sum(contribution_margin_in_usd_parallel) as cm_in_usd_parallel
        from {{ref('contribution_margin_details')}}
        Group by user_id

    ), is_whale as (

    select distinct monetized_user_id as user_id, True as is_whale 
    from dbt_transformations_looker.whales_monthly
    where is_whale = true 

    ), is_parallel_whale as (

    select distinct  monetized_user_id as user_id, True as is_parallel_whale 
    from dbt_transformations_looker.whales_monthly
    where is_parallel_whale = true 
    
    ), all_users as (
    
    Select 
        u.user_id, 
        u.is_admin, 
        u.is_internal, 
        u.is_deleted, 
        u.is_business,
        u.is_valid_user,
        u.is_blocked_by_flag, 
        u.primary_currency, 
        u.acquisition_source, 
        u.has_risk_flag, 
        u.kyc_tier,
        coalesce(w.is_whale, false ) as is_whale, 
        coalesce(pw.is_parallel_whale, false) as is_parallel_whale,
        df.is_monetized_user,
        date_trunc({{date_granularity}},u.created_at) as acquisition_cohort
    from {{ref('expanded_users')}} u
    inner join {{ref('user_demographic_features')}} df
        on df.user_id = u.user_id
    left join is_whale w 
        on u.user_id = w.user_id 
    left join is_parallel_whale pw 
        on u.user_id = pw.user_id

), users_agg as (

    Select 
        u.acquisition_cohort, 
        datediff({{date_granularity}}, u.acquisition_cohort, m.date) as time_cohort_alive,  
        u.is_admin, 
        u.is_internal, 
        u.is_deleted, 
        u.is_business,
        u.is_valid_user,
        u.has_risk_flag,
        u.kyc_tier,
        u.is_blocked_by_flag, 
        u.primary_currency, 
        u.acquisition_source,
        u.is_monetized_user,
        u.is_whale, 
        u.is_parallel_whale,
        count(distinct u.user_id) as total_users_in_cohort,
        sum(cm_in_usd) as cohort_total_cm_in_usd,
        sum(cm_in_usd_parallel) as cohort_total_cm_in_usd_parallel
    from  dim_dates m
    Left Join all_users u 
        on m.date >= u.acquisition_cohort
    Left Join cm 
        on u.user_id = cm.user_id
    {{ dbt_utils.group_by(n=15) }}

), transacting_users as (

   Select  
        distinct 
        u.user_id,
        date_trunc({{date_granularity}}, u.created_at) as acquisition_cohort, 
        floor(
            Div0(
                datediff('day', u.created_at, l.hlo_updated_at), 
                {{date_mapping[date_granularity][1]}})) as transaction_time_since_acquisition,
        u.is_admin,
        u.is_internal,
        u.is_deleted, 
        u.is_business, 
        u.is_valid_user,
        u.is_blocked_by_flag, 
        u.has_risk_flag,
        u.primary_currency, 
        u.acquisition_source,
        u.kyc_tier,
        coalesce(w.is_whale, false ) as is_whale, 
        coalesce(pw.is_parallel_whale, false) as is_parallel_whale,
        df.is_monetized_user,
        cm.cm_in_usd,
        cm.cm_in_usd_parallel
    from {{ref('expanded_ledgers')}} l 
    join {{ref('expanded_users')}} u 
        on u.user_id = l.main_party_user_id
    inner join {{ref('user_demographic_features')}} df
        on df.user_id = u.user_id
    left join cm 
        on l.main_party_user_id = cm.user_id
    left join is_whale w 
        on u.user_id = w.user_id 
    left join is_parallel_whale pw 
        on u.user_id = pw.user_id
    where hlo_status in ('COMPLETED', 'SETTLED')
        and is_original_transfer_reversed = FALSE 

), transacting_users_agg as (

    Select 
        acquisition_cohort, 
        transaction_time_since_acquisition,
        is_admin,
        is_internal,
        is_deleted, 
        is_business, 
        is_valid_user,
        is_blocked_by_flag, 
        has_risk_flag,
        primary_currency, 
        acquisition_source,
        kyc_tier,
        is_monetized_user,
        is_whale, 
        is_parallel_whale,
        sum(cm_in_usd) as total_transacting_cm_in_usd,
        sum(cm_in_usd_parallel) as total_transacting_cm_in_usd_parallel,
        count(distinct user_id) as total_transacting_users
    from transacting_users
     {{ dbt_utils.group_by(n=15) }}

), final as (

    Select 
        u.acquisition_cohort:: date as acquisition_cohort, 
        u.time_cohort_alive, 
        '{{date_granularity}}' as date_granularity, 
        u.is_admin, 
        u.is_internal,
        u.is_deleted, 
        u.is_business, 
        u.is_valid_user,
        u.has_risk_flag,
        u.is_blocked_by_flag, 
        u.primary_currency, 
        u.acquisition_source,
        u.kyc_tier,
        u.is_monetized_user,
        u.is_whale, 
        u.is_parallel_whale,
        coalesce(u.total_users_in_cohort, 0) as total_users_in_cohort,
        coalesce(u.cohort_total_cm_in_usd, 0) as total_cm_in_usd,
        coalesce(u.cohort_total_cm_in_usd_parallel,0) as total_cm_in_usd_parallel,
        coalesce(t.total_transacting_users,0) as total_transacting_users,
        coalesce(t.total_transacting_cm_in_usd,0) as total_transacting_cm_in_usd,
        coalesce(t.total_transacting_cm_in_usd_parallel,0) as total_transacting_cm_in_usd_parallel
    from users_agg u 
    left join transacting_users_agg t 
        on u.acquisition_cohort = t.acquisition_cohort
        and u.time_cohort_alive = t.transaction_time_since_acquisition
        and u.is_admin = t.is_admin
        and u.is_internal = t.is_internal
        and u.is_deleted = t.is_deleted
        and u.is_business = t.is_business
        and u.is_valid_user = t.is_valid_user
        and u.is_blocked_by_flag = t.is_blocked_by_flag
        and u.primary_currency = t.primary_currency
        and u.acquisition_source = t.acquisition_source
        and u.has_risk_flag = t.has_risk_flag
        and u.kyc_tier = t.kyc_tier
        and u.is_monetized_user = t.is_monetized_user
        and u.is_whale = t.is_whale
        and u.is_parallel_whale = t.is_parallel_whale

) select * from final


{%- endset %}


{{ return(query) }}

{% endmacro %}
