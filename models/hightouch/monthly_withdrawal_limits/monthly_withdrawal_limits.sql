{{ config(materialized='table') }}

with withdrawals as (

   Select 
        l.main_party_user_id as user_id, 
        u.primary_currency, 
        date_trunc(month, l.hlo_updated_at) as transaction_month,
        count(distinct l.transfer_id) as total_withdrawals, 
        sum(abs(l.ledger_amount)) as total_amount_withdrawn 
    from dbt_transformations.expanded_ledgers l 
    join dbt_transformations.expanded_users u 
        on l.main_party_user_id = u.user_id 
    where l.hlo_status in ('COMPLETED', 'SETTLED')
        and l.transfer_type in ('WITHDRAWALS_SETTLED', 'S2NC_SETTLED')
        and u.primary_currency in ('RWF', 'UGX')
        and u.primary_currency = l.ledger_currency -- 1 instances from 08-2018 where primary currency is RWF and Ledger currency is UGX  
    Group by 1,2,3


), cm as (

Select 
    user_id, 
    sum(contribution_margin_in_usd) as cm
from dbt_transformations_looker.contribution_margin
where transaction_settled_date >= dateadd(day,-30, current_date())
Group by user_id


), final as ( 

Select w.user_id, w.primary_currency, w.transaction_month, w.total_withdrawals, w.total_amount_withdrawn, 
case 
    when w.primary_currency = 'UGX' 
        and (w.total_withdrawals >= 25 or w.total_amount_withdrawn > 6250000)
            then TRUE
    when primary_currency = 'RWF' 
        and (w.total_withdrawals >= 25 or w.total_amount_withdrawn > 2500000)
            Then TRUE
    else FALSE
End as meets_withdrawal_threshold, 
coalesce(cm,0) as cm_last_30_days 
from withdrawals w 
left join cm 
    on w.user_id = cm.user_id
where meets_withdrawal_threshold = TRUE
and cm_last_30_days < 0 

) 
Select * 
from final

