-- given that we know the link between user and any 1 amplitude ID of
-- the user, we link all the other amplitude ID's back to the user
{{ 
    config(
        materialized='table',
        schema='intermediate'
    ) 
}}
(
    select 
        distinct
        map_user_amplitude.user_id as user_id,
        merged_amplitude_ids.amplitude_id as amplitude_id
    from 
        {{ ref("map_user_amplitude") }} map_user_amplitude
        left join chipper.amplitude.merge_ids_204512 as merged_amplitude_ids
            on map_user_amplitude.amplitude_id = merged_amplitude_ids.merged_amplitude_id
    where
        merged_amplitude_ids.amplitude_id is not null
)
union 
(
    select 
        distinct
        map_user_amplitude.user_id as user_id,
        merged_amplitude_ids.merged_amplitude_id as amplitude_id
    from 
        {{ ref("map_user_amplitude") }} map_user_amplitude
        left join chipper.amplitude.merge_ids_204512 as merged_amplitude_ids
            on map_user_amplitude.amplitude_id = merged_amplitude_ids.amplitude_id
    where
        merged_amplitude_ids.merged_amplitude_id is not null
)
