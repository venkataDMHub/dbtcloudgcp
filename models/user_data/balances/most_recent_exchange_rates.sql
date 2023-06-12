{{ config(materialized='ephemeral') }}

Select
    id,
    currency,
    rate,
    timestamp As most_recent_exchange_rate_timestamp
From chipper.{{var("core_public")}}.exchange_rates
Qualify max(timestamp) Over (Partition By currency) = timestamp
