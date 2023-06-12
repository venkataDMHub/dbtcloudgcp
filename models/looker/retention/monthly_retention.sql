{{ config(materialized = 'ephemeral') }}
{{ generate_retention('Month') }}
