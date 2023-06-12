{{
    config(
        materialized='incremental',
        unique_key='user_id'
    )
}}
select 
    user_id,
    face_url,
    updated_at
from 
    chipper.{{ var("compliance_public") }}.liveness_checks
where
    status = 'ACCEPTED'   
{% if is_incremental() %}
  -- this filter will only be applied on an incremental run
  and updated_at > (select max(updated_at) from {{ this }})
{% endif %}
qualify row_number() over(partition by user_id order by created_at desc) = 1
