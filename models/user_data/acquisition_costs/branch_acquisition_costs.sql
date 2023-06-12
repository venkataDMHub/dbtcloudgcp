with branch_costs_base as (

    SELECT date, 
    ad_partner, campaign, nvl(ad_set_name, campaign) as ad_set_name, ad_set_id, sum(cost) as cost
    FROM CHIPPER.BRANCH.COSTS
    
    {{ dbt_utils.group_by(n=5) }}


), user_mapping AS (

    SELECT user_id, 
    date_trunc(day, branch_install_created_at) as date,
    acquisition_source, campaign, ad_set_name, ad_set_id
    FROM {{ ref('first_branch_install_corrected') }}
    WHERE acquisition_source != 'Direct Install'

), users_per_ad as (

    Select date,
    acquisition_source, campaign, ad_set_name, ad_set_id,
    count(distinct user_id) as total_users
    from user_mapping
    where user_id not in (select distinct user_id from {{ ref('referral_acquired_users') }}) 

    {{ dbt_utils.group_by(n=5) }}
    
)  Select
  coalesce(costs.date, installs.date) as date,
  coalesce(costs.ad_partner, installs.acquisition_source) as ad_partner,
  coalesce(costs.campaign, installs.campaign) as campaign,
  coalesce(costs.ad_set_name, installs.ad_set_name) as ad_set_name,
  coalesce(costs.ad_set_id, installs.ad_set_id) as ad_set_id,
  costs.cost as total_cost,
  coalesce(installs.total_users, 0) as total_users_installed
  from branch_costs_base as costs
  full outer join users_per_ad as installs
      on costs.date = installs.date
      and costs.ad_partner = installs.acquisition_source
      and costs. campaign = installs.campaign
      and costs.ad_set_name = installs.ad_set_name 
      
