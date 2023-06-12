with base as (
  
  Select lpv.USER_ID, lpv.TOTAL_LPV_USD, 
         lpv.PERCENT_LPV_FROM_REWARDS, 
         u.KYC_TIER, u.IS_VALID_USER
  from {{ ref('user_lifetime_processed_volume') }} lpv
  left join {{ ref('expanded_users') }} u
    on lpv.user_id = u.user_id

), non_activated_users as (

Select user_id, 
      PERCENT_LPV_FROM_REWARDS,
      TOTAL_LPV_USD, 
  Case 
    When IS_VALID_USER = false then 'Invalid User'
    When KYC_TIER = 'TIER_0' then 'Tier_0 User'
    When TOTAL_LPV_USD = 0 then 'Non-activated User'
    else null
  end as LPV_GROUP
from base
where TOTAL_LPV_USD = 0 
  or  IS_VALID_USER = False 
  or  KYC_TIER = 'TIER_0'
  
), percentiles as (
  
  Select USER_ID, 
        TOTAL_LPV_USD, 
        PERCENT_LPV_FROM_REWARDS,
    PERCENTILE_CONT (0.25) WITHIN GROUP (ORDER BY TOTAL_LPV_USD) OVER () as Q1,
    PERCENTILE_CONT (0.75) WITHIN GROUP (ORDER BY TOTAL_LPV_USD) OVER () as Q3,
    (Q3+1.5*(Q3-Q1)) as UPPER_BOUND, 
    Case 
        when TOTAL_LPV_USD >0 and TOTAL_LPV_USD <= Q1 Then 'Low Value'
        when  TOTAL_LPV_USD > Q1 and TOTAL_LPV_USD <= Q3 Then 'Middle Base'
        when TOTAL_LPV_USD > Q3 and TOTAL_LPV_USD <= UPPER_BOUND then 'High Base'
        when TOTAL_LPV_USD > UPPER_BOUND then 'Outliers'
    end as LPV_GROUP
  From base b
  WHERE TOTAL_LPV_USD > 0 
  and  IS_VALID_USER = True 
  and  KYC_TIER != 'TIER_0'

), outlier_group as (
  
  Select USER_ID, 
        PERCENT_LPV_FROM_REWARDS,
        TOTAL_LPV_USD, 
    PERCENTILE_CONT (0.25) WITHIN GROUP (ORDER BY TOTAL_LPV_USD) OVER () as Q1o,
    PERCENTILE_CONT (0.75) WITHIN GROUP (ORDER BY TOTAL_LPV_USD) OVER () as Q3o,
    Case  
      when TOTAL_LPV_USD <= Q1o Then 'Slight Outlier'
      when TOTAL_LPV_USD > Q1o and TOTAL_LPV_USD <= Q3o Then 'Outlier Base'
      when TOTAL_LPV_USD > Q3o then 'Top User'
      else null 
    end as LPV_GROUP
  from percentiles
  Where LPV_GROUP = 'Outliers'
  
), combined as (
    
    Select USER_ID, TOTAL_LPV_USD, PERCENT_LPV_FROM_REWARDS, LPV_GROUP
    From non_activated_users

    union 

    Select USER_ID, TOTAL_LPV_USD,  PERCENT_LPV_FROM_REWARDS, LPV_GROUP
    From percentiles
    Where LPV_GROUP != 'Outliers'

    union 

    Select USER_ID, TOTAL_LPV_USD,  PERCENT_LPV_FROM_REWARDS, LPV_GROUP
    From outlier_group
    
), final as (
    
    Select USER_ID, 
           TOTAL_LPV_USD,
           PERCENT_LPV_FROM_REWARDS,
           LPV_GROUP,
           min(TOTAL_LPV_USD) over(partition by LPV_GROUP) as LPV_RANGE_MIN,
           max(TOTAL_LPV_USD) over(partition by LPV_GROUP) as LPV_RANGE_MAX 
    from combined
) 
Select *
from final
