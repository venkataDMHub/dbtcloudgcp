{{ config(materialized = 'ephemeral') }}
{{ generate_retention('Quarter') }}
