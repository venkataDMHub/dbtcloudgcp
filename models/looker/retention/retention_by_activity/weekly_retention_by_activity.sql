 -- depends_on: {{ ref('dim_dates') }}

{{ config(materialized = 'ephemeral') }}
{{ generate_retention_by_activity('Week') }}
