{{ config(materialized = 'ephemeral') }}

with device_ids_1 as (

    select
        user_id,
        os_version,
        device_type,
        updated_at as last_updated_at
    from chipper.{{ var("core_public") }}.user_device_ids
    qualify max(updated_at) over (partition by user_id) = updated_at

),

device_ids_2 as (

    select
        user_id as user_id,
        provider_response,
        timestamp as last_updated_at,
        get(provider_response, 'attributes') as attributes,
        trim(get(attributes, 'Model')::string, '[], ""') as device_type,
        trim(get(attributes, 'OS')::string, '[], ""') as os_version
    from chipper.{{ var("core_public") }}.device_fingerprints
    qualify max(timestamp) over (partition by user_id) = timestamp

),

combined as (

    select
        user_id,
        os_version,
        device_type,
        last_updated_at
    from device_ids_1

    union

    select
        user_id,
        os_version,
        device_type,
        last_updated_at
    from device_ids_2

),

cleaned_device_ids as (

    select
        user_id,
        device_type,
        os_version,
        last_updated_at,
        case
            when os_version like '%android%' or os_version like 'Android' then 'android'
            when os_version like '%ios%' or os_version like '%iOS%' then 'ios'
            when os_version is null then 'N/A'
            else os_version
        end as platform
    from combined
    where iff(os_version is null and device_type is null, True, False) = False
    qualify max(last_updated_at) over (partition by user_id) = last_updated_at
)

select * from cleaned_device_ids
