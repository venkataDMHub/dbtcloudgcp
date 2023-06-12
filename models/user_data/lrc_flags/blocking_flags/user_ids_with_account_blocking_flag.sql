{{ config(materialized='ephemeral') }}

{% set account_blocking_flags = (
        'USER_LOCKED',
        'USER_OFFBOARDED',
        'BLOCKED_PEP',
        'POTENTIAL_SANCTIONS_MATCH',
        'CONFIRMED_SANCTIONS_MATCH'   
    )
%}

with user_ids_with_account_blocking_flag as (
    select 
        user_id, 
        flag
    from {{var("compliance_public")}}.account_flags
    where 
        flag IN {{ account_blocking_flags }}
        and date_unflagged is NULL
)

select 
    user_id,
    listagg(distinct flag, ', ') within group (order by flag) as flag
from user_ids_with_account_blocking_flag
group by user_id
