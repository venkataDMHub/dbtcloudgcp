{{ config(materialized = 'table') }} 

with latest_user_engagement as (

    select *
    from chipper.utils.user_engagement_score
    qualify max(updated_at) over (partition by user_id) = updated_at

),

qualified_users as (

    select
        users.user_id,
        users.primary_currency,
        users.gender,
        users.user_age,
        users.acquisition_source,
        users.purpose_of_account,
        users.account_age as account_age_in_days,
        users.postal_code_latest,
        users.city_latest,
        users.uos_score,
        users.total_lpv_usd,
        users.percent_lpv_from_rewards,
        users.lpv_group,
        users.latest_engagement_bucket,
        users.latest_engagement_score,
        device.platform,
        has_linked_account, 
        has_verified_linked_account,
        ues.p2p_score,
        ues.purchases_score,
        ues.deposits_score,
        ues.investments_score,
        coalesce(network.number_users_referred,0) as number_users_referred,
        coalesce(network.total_p2p_network,0) as total_p2p_network,
        crypto.average_days_to_next_crypto_outflow, 
        stock.average_days_between_stock_sells_and_buys,
        date_trunc(month, users.acquisition_date) as acquisition_cohort,
        IFF(users.purpose_of_account ilike '%AIRTIME_AND_BILLS%' 
            or users.purpose_of_account ilike '%AIRTIME_AND_OR_BILLS%',
            True, False) as POA_airtime_or_bills, 
        IFF(users.purpose_of_account ilike '%BUSINESS_ACTIVITIES%' 
            or users.purpose_of_account ilike '%MY_BUSINESS%',
            True, False) as POA_business,
            
        IFF(users.purpose_of_account ilike '%BUY_OR_SELL_GOODS_INVESTMENTS%' 
            or users.purpose_of_account ilike '%SAVINGS_AND_INVESTING%',
            True, False) as POA_investments,
            
        IFF(users.purpose_of_account ilike '%SAVINGS%' 
            or users.purpose_of_account ilike '%SAVINGS_AND_INVESTING%',
            True, False) as POA_savings,
            
        IFF(users.purpose_of_account ilike '%LIVING_COSTS_OR_FAMILY_SUPPORT%' 
            or users.purpose_of_account ilike '%FAMILY_SUPPORT%',
            True, False) as POA_family_support, 
            
        IFF(users.purpose_of_account ilike '%LIVING_COSTS_OR_FAMILY_SUPPORT%' 
            or users.purpose_of_account ilike '%GOODS_AND_SERVICES%',
            True, False) as POA_living_costs, 
            
        IFF(users.purpose_of_account ilike '%PAY_FAMILY_AND_FRIENDS_ABROAD%' 
            or users.purpose_of_account ilike '%PAY_FAMILY_AND_FRIENDS_IN_COUNTRY%',
            True, False) as POA_pay_friends_family, 
            
        IFF(users.purpose_of_account ilike '%BUY_OR_SELL_GOODS_INVESTMENTS%', 
            True, False) as POA_buy_sell_goods, 
            
        IFF(users.purpose_of_account ilike '%BUY_OR_SELL_PROPERTY%', 
            True, False) as POA_buy_sell_property,
            
        IFF(users.purpose_of_account ilike '%OTHER%', 
            True, False) as POA_other,
        case 
            when users.uos_score >= 0.8 then 'High Risk'
            when users.uos_score > 0.3 then 'Medium Risk'
            when users.uos_score <= 0.3 then 'Low Risk'
            else 'NOT_AVAILABLE'
        end as UOS_score_buckets,     
        case
            when ues.p2p_score = 0 then 'INACTIVE'
            when ues.p2p_score > 0 and ues.p2p_score <= 0.3 then 'LOW'
            when ues.p2p_score > 0.3 and ues.p2p_score <= 0.7 then 'MEDIUM'
            when ues.p2p_score > 0.7 and ues.p2p_score <= 1 then 'HIGH'
        end as p2p_bucket,
        case
            when ues.purchases_score = 0 then 'INACTIVE'
            when ues.purchases_score > 0 and ues.purchases_score <= 0.3 then 'LOW'
            when ues.purchases_score > 0.3 and ues.purchases_score <= 0.7 then 'MEDIUM'
            when ues.purchases_score > 0.7 and ues.purchases_score <= 1 then 'HIGH'
        end as purchases_bucket,
        case
            when ues.deposits_score = 0 then 'INACTIVE'
            when ues.deposits_score > 0 and ues.deposits_score <= 0.3 then 'LOW'
            when ues.deposits_score > 0.3 and ues.deposits_score <= 0.7 then 'MEDIUM'
            when ues.deposits_score > 0.7 and ues.deposits_score <= 1 then 'HIGH'
        end as deposits_bucket,
        case
            when ues.investments_score = 0 then 'INACTIVE'
            when ues.investments_score > 0 and ues.investments_score <= 0.3 then 'LOW'
            when ues.investments_score > 0.3 and ues.investments_score <= 0.7 then 'MEDIUM'
            when ues.investments_score > 0.7 and ues.investments_score <= 1 then 'HIGH'
        end as investments_bucket,
        case
            when users.user_age < 10 or users.user_age >= 80 then 'N/A'
            when users.user_age >= 10 and users.user_age < 20 then '10-19'
            when users.user_age >= 20 and users.user_age < 30 then '20-29'
            when users.user_age >= 30 and users.user_age < 40 then '30-39'
            when users.user_age >= 40 and users.user_age < 50 then '40-49'
            when users.user_age >= 50 and users.user_age < 60 then '50-59'
            when users.user_age >= 60 and users.user_age < 70 then '60-69'
            when users.user_age >= 70 and users.user_age < 80 then '70-79'
            else 'N/A'
        end as user_age_bucket,
        case
            when users.phone_number is not null
                then 1
            else 0
        end as provided_phone_number,
        case
            when users.email_address is not null
                then 1
            else 0
        end as provided_email
    from {{ ref('user_demographic_features') }} as users
    left join {{ ref('devices_corrected') }} as device
        on users.user_id = device.user_id
    left join {{ ref('user_accounts') }} as accounts
            on users.user_id = accounts.user_id
    left join latest_user_engagement as ues
            on users.user_id = ues.user_id
    left join {{ ref('user_networks') }} as network
            on users.user_id = network.user_id
    left join  {{ ref('average_days_to_crypto_outflows') }} crypto 
            on users.user_id = crypto.user_id 
    left join  {{ ref('average_days_to_stock_outflows') }} stock 
            on users.user_id = stock.user_id 

        where
            users.primary_currency in ('NGN', 'UGX')
            and users.is_valid_user = TRUE
            and users.is_deleted = FALSE
            and users.is_admin = FALSE
            and users.is_blocked_by_flag = FALSE
            and users.is_internal = FALSE
            and users.is_business = FALSE
            and users.has_risk_flag = FALSE  
            AND users.kyc_tier in ({{ verified_tiers() }})
            and users.uos_score <= 0.7
            and round(users.percent_lpv_from_rewards, 0) not in (25, 50, 33, 100)
   
)
select *
from qualified_users
