{{
    config(
        materialized='table',
        schema='intermediate'
    )
}}

with device_rank_fingerprints as (

    select
        to_varchar(user_id) as user_id,
        to_varchar(device_id) as device_id,
        to_varchar(trim(provider_response:attributes.Model[0], '""'))  as device_model,
        to_varchar(trim(provider_response:attributes.OS[0], '""')) as os,
        to_varchar(provider_response:signals.TrueIP[0]) as ip_address,
        timestamp as updated_at
    from chipper.{{ var("core_public") }}.device_fingerprints

), device_rank_user_devices as (

    Select
        to_varchar(user_id) as user_id, 
        to_varchar(device_id) as device_id,
        to_varchar(device_Type) as device_model,
        Case 
            when OS_VERSION like '%android%' or OS_VERSION like 'Android' then 'Android'
            when OS_VERSION like '%ios%' or OS_VERSION like '%iOS%' then 'iOS'
            when OS_VERSION is null then null
            else 'Other'
        end as os, 
        to_varchar(MOST_RECENT_IP) as ip_address, 
        updated_at
    from chipper.{{ var("core_public") }}.user_device_ids

), combined as (

    select user_id, device_id, device_model, os, ip_address, updated_at
    from device_rank_fingerprints

        union

    select user_id, device_id, device_model, os, ip_address, updated_at
    from device_rank_user_devices

), combined_cleaned as (

    Select 
        user_id, 
        device_id, 
        device_model, 
        os,
        ip_address, 
        updated_at,
        row_number() over (partition by user_id order by updated_at asc) as row_num_first,
        row_number() over (partition by user_id order by updated_at desc) as row_num_latest
    from combined
    where iff(os is null and device_model is null, True, False) = FALSE

), device_first as (
    select
        user_id,
        device_id as device_id_first,
        device_model as device_model_first,
        os as os_first,
        ip_address as ip_address_first
    from combined_cleaned
    where row_num_first = 1

), device_latest as (
    select
        user_id,
        device_id as device_id_latest,
        device_model as device_model_latest,
        os as os_latest,
        ip_address as ip_address_latest
    from combined_cleaned
    where row_num_latest = 1
), app_version_ranked as (
    select 
        user_id,
        (case
            when app_version like '%.%.%' 
             and app_version not like '%rm%' 
             and app_version not like '%apk%' then app_version
            else 'test'
           end) as app_version,
        row_number() over (partition by user_id order by updated_at asc) as row_num_first,
        row_number() over (partition by user_id order by updated_at desc) as row_num_latest
    from chipper.{{ var("core_public") }}.user_device_ids
), user_app_version_first as (
    select
        user_id,
        app_version as app_version_first
    from app_version_ranked
    where row_num_first = 1
), user_app_version_latest as (
    select
        user_id,
        app_version as app_version_latest
    from app_version_ranked
    where row_num_latest = 1
)

select
    device_first.user_id,
    device_first.device_id_first,
    device_first.device_model_first,
    device_first.os_first,
    device_first.ip_address_first,
    user_app_version_first.app_version_first,
    device_latest.device_id_latest,
    device_latest.device_model_latest,
    device_latest.os_latest,
    device_latest.ip_address_latest,
    user_app_version_latest.app_version_latest
from device_first 
    join device_latest on device_first.user_id = device_latest.user_id
    join user_app_version_first on device_first.user_id = user_app_version_first.user_id
    join user_app_version_latest on device_first.user_id = user_app_version_latest.user_id
