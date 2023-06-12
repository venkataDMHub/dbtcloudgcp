{{ config(materialized='ephemeral') }}

select 
    *
from {{ref('deposits_original_transfer_details')}}
union
select 
    *
from {{ref('deposits_reverse_transfer_details')}}
