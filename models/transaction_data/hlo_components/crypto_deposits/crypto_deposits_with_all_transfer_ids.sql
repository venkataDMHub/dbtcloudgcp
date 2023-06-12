{{  config(
        materialized='incremental',
        unique_key='transfer_id',
        on_schema_change='append_new_columns', 
        schema='intermediate') }}


select
    id::text as hlo_id,
    'CRYPTO_DEPOSITS' as hlo_table,
    journal_id as hlo_journal_id,
    status as hlo_status,
    created_at as hlo_created_at,
    updated_at as hlo_updated_at,
    transfer_id as transfer_id,
    FALSE as is_original_transfer_reversed,
    FALSE as is_transfer_reversal,
    PROVIDER as external_provider,
    PROVIDER_TRANSACTION_ID as external_provider_transaction_id,
    case
        when is_transfer_reversal = TRUE then concat(hlo_table, '_', 'REVERSAL')
        else concat(hlo_table, '_', hlo_status)
    end as transfer_type,
    object_construct(
        '_internalTransactionDetails',object_construct(
            'asset', coalesce(asset, try_parse_json('NULL')),
            'address', coalesce(address,try_parse_json('NULL')),
            'transactionHash', coalesce(transaction_hash,try_parse_json('NULL')),
            'fee',coalesce(fee::number(10,2),try_parse_json('NULL')),
            'reversalProviderTransactionId' ,coalesce(REVERSAL_PROVIDER_TRANSACTION_ID,try_parse_json('NULL')),
            'reversalTransactionHash',coalesce(REVERSAL_TRANSACTION_HASH,try_parse_json('NULL')),
            'reversalAddress',coalesce(REVERSAL_ADDRESS,try_parse_json('NULL'))
        ),
        'externalProviderConfirmationDetails',coalesce(confirmation_details,try_parse_json('NULL')),
        'externalProviderTransactionDetails',coalesce(PROVIDER_DETAILS,try_parse_json('NULL'))
    ) as transaction_details,
    concat(
        'Deposited ',
        transaction_details:"_internalTransactionDetails":"asset",
        ' to address owned by the user on Chipper: ',
        transaction_details:"_internalTransactionDetails":"address"
      ) as shortened_transaction_details,
    null as outgoing_user_id,
    user_id as incoming_user_id

from "CHIPPER".{{ var("core_public") }}."CRYPTO_DEPOSITS"
where transfer_id is not null
{% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    and hlo_updated_at >= (select max(hlo_updated_at) from {{ this }})
{% endif %}
