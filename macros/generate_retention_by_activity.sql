{% macro generate_retention_by_activity(date_granularity) %}

{# Dictionary that maps date granularity to dim_dates timeframe to days_joined_since to qualify for activation #}
{% set date_mapping= {'Month' : ['FIRST_DAY_OF_MONTH', 30], 
                        'Week' : ['FIRST_DAY_OF_WEEK', 7],
                        'Quarter': ['FIRST_DAY_OF_QUARTER', 90]
                        }
%}

{% set transfer_type = ('AIRTIME_PURCHASES_COMPLETED', 'ASSET_TRADES_BUY_SETTLED','ASSET_TRADES_SELL_SETTLED',
'BILL_PAYMENTS_COMPLETED','CHECKOUTS_SETTLED', 'CRYPTO_DEPOSITS_SETTLED', 'CRYPTO_WITHDRAWALS_SETTLED',
'DATA_PURCHASES_COMPLETED','DEPOSITS_SETTLED', 'ISSUED_CARD_TRANSACTIONS_FUNDING_COMPLETED','ISSUED_CARD_TRANSACTIONS_WITHDRAWAL_COMPLETED',
'NETWORK_API_B2C_SETTLED','NETWORK_API_C2B_SETTLED', 'PAYMENTS_P2P_SETTLED','PAYMENT_INVITATIONS_SETTLED', 'REQUESTS_SETTLED', 
'S2NC_SETTLED','STOCK_TRADES_BUY_SETTLED', 'STOCK_TRADES_DIVTAX_SETTLED','STOCK_TRADES_DIV_SETTLED','STOCK_TRADES_SELL_SETTLED',
 'WITHDRAWALS_SETTLED')  %}

{% set query %}

    with dim_dates as (

    Select distinct 
        dd.{{date_mapping[date_granularity][0]}} as date
    from {{ref('dim_dates')}}  dd
    where dd.{{date_mapping[date_granularity][0]}} <= current_date

    ), transfers as (
        
        Select transfer_id, transfer_type, activity_type, main_party_user_id, hlo_updated_at ,
         Case 
            when l.activity_type in ('B2C_RECEIVED', 'B2C_SENT', 'C2B_RECEIVED', 'C2B_SENT') then 'NETWORK_API'
            when l.activity_type in ('CRYPTO_DEPOSITS', 'CRYPTO_WITHDRAWALS','ASSET_TRADES_BUY', 'ASSET_TRADES_SELL')
                then 'CRYPTO'
            when l.activity_type in ('ISSUED_CARD_TRANSACTIONS_FUNDING', 'ISSUED_CARD_TRANSACTIONS_WITHDRAWAL') 
                then 'CARD'
            when l.activity_type in ('P2P_RECEIVED_LOCAL','P2P_SENT_LOCAL') then 'P2P_LOCAL'
            when l.activity_type in ('P2P_RECEIVED_CROSS_BORDER','P2P_SENT_CROSS_BORDER' ) then 'P2P_CROSS_BORDER'
            when l.activity_type in ('STOCK_TRADES_BUY', 'STOCK_TRADES_DIV', 'STOCK_TRADES_DIVTAX', 'STOCK_TRADES_SELL')                 then 'STOCKS'
            else  l.activity_type 
        end as  activity_category 
        from  dbt_transformations_looker.ledgers_details l
        where is_original_transfer_reversed = FALSE
        and hlo_status in ('COMPLETED', 'SETTLED')
        and transfer_type in {{transfer_type}}

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
        df.is_monetized_user,
        l.activity_category,
        date_trunc({{date_granularity}},u.created_at) as acquisition_cohort
    from {{ref('expanded_users')}} u
    inner join {{ref('user_demographic_features')}} df
        on df.user_id = u.user_id
    left join transfers l
        on u.user_id = l.main_party_user_id
        

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
        coalesce(u.activity_category,'NONE') as activity_category,
        count(distinct u.user_id) as total_users_in_cohort
    from  dim_dates m
    Left Join all_users u 
        on m.date >= u.acquisition_cohort
    {{ dbt_utils.group_by(n=14) }}
    Order by 1 desc,2

), transacting_users as (

   Select  
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
        df.is_monetized_user,
        Coalesce(l.activity_category,'NONE') as  activity_category,
        count(distinct u.user_id) as total_transacting_users
    from transfers l 
    inner join {{ref('expanded_users')}} u 
        on u.user_id = l.main_party_user_id
    inner join {{ref('user_demographic_features')}} df
        on df.user_id = u.user_id
    {{ dbt_utils.group_by(n=14) }}
    order by 1 desc, 2
    
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
        u.activity_category,
        coalesce(u.total_users_in_cohort, 0) as total_users_in_cohort,
        coalesce(t.total_transacting_users,0) as total_transacting_users
    from users_agg u 
    left join transacting_users t 
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
        and (u.activity_category = t.activity_category or t.activity_category is null)

) select * from final


{%- endset %}


{{ return(query) }}

{% endmacro %}
