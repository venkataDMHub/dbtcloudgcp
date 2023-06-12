{{ dbt_utils.union_relations(
    relations=[
        ref('map_user_amplitude'), 
        ref('map_user_to_all_amplitude_ids'), 
    ],
) }}
