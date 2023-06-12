{{ config(materialized='table') }}

select 
    user_id,
    array_agg(distinct flag) as all_active_flags,
    count(distinct flag) as num_flags
from 
    chipper.{{ var('compliance_public') }}.account_flags
where
    date_unflagged is null
group by 
    user_id
