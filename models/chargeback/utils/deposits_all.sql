{{ config(materialized='view') }}

{{ dbt_utils.union_relations(
    relations=[ref('deposits_using_banks'), ref('deposits_using_cards')]
) }}
