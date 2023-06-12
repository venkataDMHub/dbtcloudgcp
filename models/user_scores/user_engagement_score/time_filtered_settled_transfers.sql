{{ config(materialized='ephemeral') }}

SELECT
    *
FROM
    {{ ref('expanded_ledgers') }}
WHERE
    CAST(
        hlo_updated_at AS DATE
    ) > DATEADD(
        days,
        {{var("ues_time_horizon_in_days")}},
        CURRENT_TIMESTAMP()
    )
and is_original_transfer_reversed = false
