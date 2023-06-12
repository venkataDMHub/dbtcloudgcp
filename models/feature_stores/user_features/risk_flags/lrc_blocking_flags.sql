{{ config(materialized='view') }}

{% set account_blocking_flags = (        
        'USER_LOCKED',
        'USER_OFFBOARDED',
        'BLOCKED_PEP',
        'POTENTIAL_SANCTIONS_MATCH',
        'CONFIRMED_SANCTIONS_MATCH'   
    )
%}


select 
    distinct user_id, 
    'LRC_BLOCKING' as risk_type,
    flag as risk_reason
from {{ var('compliance_public') }}.account_flags
where 
    flag IN {{ account_blocking_flags }}
    and date_unflagged is NULL
