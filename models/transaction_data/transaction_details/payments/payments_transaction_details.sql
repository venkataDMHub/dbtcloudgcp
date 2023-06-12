{{  config(
        materialized='incremental',
        unique_key='transfer_id',
        on_schema_change='append_new_columns', 
        schema='intermediate') }}

select     
    transfer_id,
    'PAYMENTS' as hlo_table,
    case when deposit_webhooks_receipts.payment_id is not null then deposit_webhooks_receipts.provider 
        else null
    end as external_provider,
    case when deposit_webhooks_receipts.payment_id is not null then deposit_webhooks_receipts.external_transaction_id 
        else null
    end as external_provider_transaction_id,
    object_construct(
        '_internalTransactionDetails', object_construct(
            'note',coalesce(note,try_parse_json('NULL')),
            'errorMessage',coalesce(error_message,try_parse_json('NULL')),
            'shortId',coalesce(short_id,try_parse_json('NULL')),
            'reference',coalesce(reference,try_parse_json('NULL')),
            'paymentGrouping',coalesce(payment_grouping,try_parse_json('NULL'))
        ),
        'externalProviderTransactionDetails', coalesce(deposit_webhooks_receipts.request_body, try_parse_json('NULL'))
    ) as transaction_details,
    COALESCE(transaction_details:"_internalTransactionDetails":"note",transaction_details:"_internalTransactionDetails":"paymentGrouping")
    as shortened_transaction_details,
    payments.created_at as hlo_created_at,
    payments.updated_at as hlo_updated_at,
    payments.status as hlo_status,
    payments.sender_id as outgoing_user_id,
    payments.recipient_id as incoming_user_id
from "CHIPPER".{{ var("core_public") }}."PAYMENTS" as payments
left join "CHIPPER".{{ var("core_public") }}."DEPOSIT_WEBHOOKS_RECEIPTS" as deposit_webhooks_receipts 
    on payments.id = deposit_webhooks_receipts.payment_id
where payments.transfer_id NOT IN (
        select transfer_id from {{ref('data_purchases_with_all_transfer_ids')}} where is_transfer_reversal = true
        union
        select transfer_id from {{ref('withdrawals_with_all_transfer_ids')}} where is_transfer_reversal = true
    )
{% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    and hlo_updated_at >= (select max(hlo_updated_at) from {{ this }})
{% endif %}