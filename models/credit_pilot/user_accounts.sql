{{ config(materialized = 'ephemeral') }}

with user_accounts as (

    select
        user_id, 
        Case 
            when count(distinct iff(payment_method_details:isLinked = true, linked_account_id, null)) > 1 
                then True 
            else False 
        end as has_linked_account,
        Case 
            when count(distinct iff(payment_method_details:isLinked = true 
                and payment_method_details:isVerified = true, linked_account_id, null)) > 1 
                    then True 
            else False 
        end as has_verified_linked_account
    from {{ ref('payment_methods') }}
    group by 1

)

select *
from user_accounts
