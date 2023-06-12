{{ config(materialized='ephemeral') }}

select
    user_id,
    device_id,
    advertising_id,
    created_at as user_device_created_at,
    updated_at as user_device_updated_at,
    row_number() over(
        partition by user_id
        order by created_at asc
    ) as row_num_asc,
    row_number() over(
        partition by user_id
        order by created_at desc
    ) as row_num_desc
from chipper.{{ var("core_public") }}.user_device_ids
