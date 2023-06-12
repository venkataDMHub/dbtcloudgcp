{{ config(materialized = 'ephemeral') }}
{{ generate_retention('Week') }}


