{{ config(materialized='ephemeral') }}

select
    user_id,
    device_id,
    advertising_id,
    user_device_created_at,
    user_device_updated_at as row_num_asc,
    row_num_desc
from {{ ref('ranked_user_device_ids') }}
where row_num_desc = 1
