{{  config(
        materialized='incremental',
        unique_key='id',
        on_schema_change='append_new_columns', 
        schema='intermediate') }}

with transfers as (
    select *
    from {{ ref('transfers_without_rate_ids') }}
    {% if is_incremental() %}
-- this filter will only be applied on an incremental run
where updated_at >= (select max(updated_at) from {{ this }})
{% endif %}
    union
    select *
    from {{ ref('transfers_with_rate_ids') }}
    {% if is_incremental() %}
-- this filter will only be applied on an incremental run
where updated_at >= (select max(updated_at) from {{ this }})
{% endif %}
),
transfers_with_same_currencies as (
    select transfers.*,
        case when origin_assets.type = 'FIAT_CURRENCY' and destination_assets.type = 'FIAT_CURRENCY' then 'LOCAL_FIAT'
            when origin_assets.type = 'CRYPTO_CURRENCY' and destination_assets.type = 'CRYPTO_CURRENCY' then 'LOCAL_CRYPTO'
            else 'LOCAL_OTHER'
        end as corridor
    from transfers
    join chipper.{{var("core_public")}}.assets as origin_assets on transfers.origin_currency = origin_assets.id
    join chipper.{{var("core_public")}}.assets as destination_assets on transfers.destination_currency = destination_assets.id
    where origin_currency = destination_currency
),
transfers_with_different_currencies as (
    select transfers.*,
        case when origin_assets.type = 'FIAT_CURRENCY' and destination_assets.type = 'FIAT_CURRENCY' then 'CROSS_BORDER_FIAT'
            when origin_assets.type = 'CRYPTO_CURRENCY' and destination_assets.type = 'CRYPTO_CURRENCY' then 'CRYPTO_TRADE'
            when origin_assets.type = 'FIAT_CURRENCY' and destination_assets.type = 'CRYPTO_CURRENCY' then 'CRYPTO_TRADE'
            when origin_assets.type = 'CRYPTO_CURRENCY' and destination_assets.type = 'FIAT_CURRENCY' then 'CRYPTO_TRADE'
            else 'CROSS_BORDER_OTHER'
        end as corridor
    from transfers
    join chipper.{{var("core_public")}}.assets as origin_assets on transfers.origin_currency = origin_assets.id
    join chipper.{{var("core_public")}}.assets as destination_assets on transfers.destination_currency = destination_assets.id
    where origin_currency != destination_currency
)
select * from transfers_with_same_currencies
union
select * from transfers_with_different_currencies
