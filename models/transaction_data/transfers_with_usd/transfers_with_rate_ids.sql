{{  config(
        materialized='incremental',
        unique_key='id',
        on_schema_change='append_new_columns', 
        schema='intermediate') }}

{% set last_N_hour = -12 %}

SELECT
    transfers.id,
    transfers.journal_id,
    transfers.origin_currency,
    transfers.origin_amount,
    transfers.exchange_rate,
    transfers.destination_currency,
    transfers.destination_amount,
    transfers.status,
    transfers.created_at,
    transfers.updated_at,
    transfers.exchange_rate_fee_percentage,
    transfers.origin_rate_id,
    transfers.destination_rate_id,
    transfers.flat_fee_amount,
    transfers.flat_fee_currency,
    transfers.base_modification_percentage,
    origin_rates.rate AS origin_rate,
    destination_rates.rate AS destination_rate
FROM chipper.{{var("core_public")}}.transfers AS transfers
LEFT JOIN
    chipper.{{var("core_public")}}.exchange_rates AS origin_rates ON
        transfers.origin_rate_id = origin_rates.id
LEFT JOIN
    chipper.{{var("core_public")}}.exchange_rates AS destination_rates ON
        transfers.destination_rate_id = destination_rates.id
WHERE origin_rate_id IS NOT NULL
    AND destination_rate_id IS NOT NULL
    AND origin_rate IS NOT NULL
    AND destination_rate IS NOT NULL

    {% if is_incremental() %}
        -- this filter will only be applied on an incremental run
        AND updated_at >= dateadd('hour', {{ last_N_hour }}, current_timestamp())
    {% endif %}
    ORDER BY transfers.id
