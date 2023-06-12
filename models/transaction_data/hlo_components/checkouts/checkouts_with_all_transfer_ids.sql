{{  config(
        materialized='incremental',
        unique_key='transfer_id',
        on_schema_change='append_new_columns', 
        schema='intermediate') }}

select
    id::text as hlo_id,
    'CHECKOUTS' as hlo_table,
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
        when is_transfer_reversal = TRUE then concat(hlo_table, '_', 'REVERSAL')
        else concat(hlo_table, '_', hlo_status)
    end as transfer_type,
    object_construct(
        '_internalTransactionDetails',object_construct(
            'note', coalesce(note, try_parse_json('NULL')),
            'payeeAccountNumber', coalesce(payee_account_number,try_parse_json('NULL')),
            'chargeId',coalesce(charge_id,try_parse_json('NULL'))
        ),
		'externalProviderTransactionDetails',try_parse_json('NULL')
    ) as transaction_details,
    transaction_details:"_internalTransactionDetails":"note" as shortened_transaction_details,
    null as outgoing_user_id,
    recipient_id as incoming_user_id
from "CHIPPER".{{ var("core_public") }}."CHECKOUTS"
where transfer_id is not null
{% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    and hlo_updated_at >= (select max(hlo_updated_at) from {{ this }})
{% endif %}
