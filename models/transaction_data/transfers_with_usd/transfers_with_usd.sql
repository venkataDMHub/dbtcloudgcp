{{  config(
        materialized='incremental',
        unique_key='id',
        on_schema_change='append_new_columns')
        }}

select
    transfers.id,
    transfers.journal_id,
    transfers.origin_currency,
    transfers.origin_amount,
    transfers.exchange_rate,
    transfers.destination_currency,
    transfers.destination_amount,
    transfers.status,
    transfers.corridor,
    transfers.created_at,
    transfers.updated_at,
    transfers.exchange_rate_fee_percentage,
    transfers.origin_rate_id,
    transfers.destination_rate_id,
    transfers.flat_fee_amount,
    transfers.flat_fee_currency,
    transfers.base_modification_percentage,
    origin_rate,
    destination_rate,
    case
        when flat_fee_currency = origin_currency then origin_rate
        else destination_rate
    end as flat_fee_rate,
    origin_rate * origin_amount as origin_amount_in_usd,
    destination_rate * destination_amount as destination_amount_in_usd,
    flat_fee_rate * flat_fee_amount as flat_fee_amount_in_usd
from {{ ref('transfers_with_assets') }} as transfers
{% if is_incremental() %}
-- this filter will only be applied on an incremental run
where updated_at >= (select max(updated_at) from {{ this }})
{% endif %}
order by id
