{{  config(
        materialized='incremental',
        unique_key='transfer_id',
        on_schema_change='append_new_columns', 
        schema='intermediate') }}

select
    id::text as hlo_id,
    'ORDERS' as hlo_table,
    journal_id as hlo_journal_id,
    status as hlo_status,
    created_at as hlo_created_at,
    updated_at as hlo_updated_at,
    transfer_id as transfer_id,
    FALSE as is_original_transfer_reversed,
    FALSE as is_transfer_reversal,
    null as external_provider,
    null as external_provider_transaction_id,
    case
        when is_transfer_reversal = TRUE then concat('NETWORK_API_C2B', '_', 'REVERSAL')
        else concat('NETWORK_API_C2B', '_', hlo_status)
    end as transfer_type,
    object_construct(
        '_internalTransactionDetails',object_construct(
            'merchantReference', coalesce(merchant_reference, try_parse_json('NULL')),
            'authorizationId', coalesce(authorisation_id,try_parse_json('NULL')),
            'shortId',coalesce(short_id,try_parse_json('NULL')),
            'note',coalesce(note,try_parse_json('NULL'))
        ),
        'externalProviderTransactionDetails',try_parse_json('NULL')
    ) as transaction_details,
    CASE WHEN transfer_type IN (
        'NETWORK_API_C2B_CANCELLED',
        'NETWORK_API_C2B_SETTLED',
        'NETWORK_API_C2B_EXPIRED'
      )
      THEN concat (
        transaction_details:"_internalTransactionDetails":"note"
      ) end as shortened_transaction_details,
    payer_id as outgoing_user_id,
    merchant_id as incoming_user_id
from "CHIPPER".{{ var("core_public") }}."ORDERS"
where transfer_id is not null
{% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    and hlo_updated_at >= (select max(hlo_updated_at) from {{ this }})
{% endif %}
