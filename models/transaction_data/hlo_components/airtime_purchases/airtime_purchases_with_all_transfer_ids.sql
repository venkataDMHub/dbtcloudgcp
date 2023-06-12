{{  config(
        materialized='incremental',
        unique_key='transfer_id',
        on_schema_change='append_new_columns', 
        schema='intermediate') }}

with all_transfer_ids as (
    select
        airtime_purchases.id::text as hlo_id,
        'AIRTIME_PURCHASES' as hlo_table,
        airtime_purchases.journal_id as hlo_journal_id,
        airtime_purchases.status as hlo_status,
        airtime_purchases.created_at as hlo_created_at,
        airtime_purchases.updated_at as hlo_updated_at,
        airtime_purchases.transfer_id as transfer_id,
        airtime_purchases.reverse_transfer_id as reverse_transfer_id,
        case
            when airtime_purchases.reverse_transfer_id is null then FALSE
            else TRUE
        end as is_original_transfer_reversed,
        AIRTIME_PROVIDER as external_provider,
        external_id as external_provider_transaction_id,
        phone_carrier,
        phone_country_code,
        phone_number,
        discount_percentage,
        commission_percentage,
        provider_response,
        case when airtime_purchases.reverse_transfer_id is null then airtime_purchases.user_id  else null end as outgoing_user_id,
        case when airtime_purchases.reverse_transfer_id is not null then airtime_purchases.user_id  else null end as incoming_user_id,

        iff(reversals_on_disbursements.transfer_id is null, false, true) as is_reverse_transfer_id_disbursement,
        iff(is_reverse_transfer_id_disbursement = false, airtime_purchases.id::text, reversals_on_disbursements.id::text) as reverse_transfer_hlo_id,
        iff(is_reverse_transfer_id_disbursement = false, 'AIRTIME_PURCHASES', 'DISBURSEMENTS') as reverse_transfer_hlo_table,
        iff(is_reverse_transfer_id_disbursement = false, airtime_purchases.journal_id, reversals_on_disbursements.journal_id) as reverse_transfer_journal_id,
        iff(is_reverse_transfer_id_disbursement = false, airtime_purchases.status, reversals_on_disbursements.status) as reverse_transfer_hlo_status,
        iff(is_reverse_transfer_id_disbursement = false, airtime_purchases.created_at, reversals_on_disbursements.created_at) as reverse_transfer_hlo_created_at,
        iff(is_reverse_transfer_id_disbursement = false, airtime_purchases.updated_at, reversals_on_disbursements.updated_at) as reverse_transfer_hlo_updated_at
        
    from {{ref('airtime_purchases')}}
    left join "CHIPPER".{{ var("core_public") }}."DISBURSEMENTS" as reversals_on_disbursements
        on airtime_purchases.reverse_transfer_id = reversals_on_disbursements.transfer_id
    where airtime_purchases.transfer_id is not null
    {% if is_incremental() %}
        -- this filter will only be applied on an incremental run
        and hlo_updated_at >= (select max(hlo_updated_at) from {{ this }})
    {% endif %}
),

transfer_ids as (
    select
        hlo_id,
        hlo_table,
        hlo_journal_id,
        hlo_status,
        hlo_created_at,
        hlo_updated_at,
        transfer_id,
        is_original_transfer_reversed,
        FALSE as is_transfer_reversal,
        external_provider,
        external_provider_transaction_id,
        case
            when is_transfer_reversal = TRUE then 'AIRTIME_PURCHASES_REVERSAL'
            else concat(hlo_table, '_', hlo_status)
        end as transfer_type,
        object_construct(
            '_internalTransactionDetails',object_construct(
                'phoneCarrier', coalesce(phone_carrier, try_parse_json('NULL')),
                'phoneCountryCode', coalesce(phone_country_code,try_parse_json('NULL')),
                'phoneNumber', coalesce(phone_number,try_parse_json('NULL')),
                'discountPercentage', coalesce(discount_percentage,try_parse_json('NULL')),
                'commissionPercentage', coalesce(commission_percentage,try_parse_json('NULL')),
                'originalTransferForReverseTransferId', coalesce(reverse_transfer_id,try_parse_json('NULL'))
            ),
            'externalProviderTransactionDetails',coalesce(provider_response,try_parse_json('NULL'))
        ) as transaction_details,
        CASE
      WHEN transfer_type IN (
        'AIRTIME_PURCHASES_REVERSAL',
        'AIRTIME_PURCHASES_QUEUED_FOR_REFUND',
        'AIRTIME_PURCHASES_PENDING',
        'AIRTIME_PURCHASES_FAILED'
      ) 
      THEN concat(
        'Attempted airtime purchase for ',
        transaction_details:"_internalTransactionDetails":"phoneCarrier",
        ' ',
        transaction_details:"_internalTransactionDetails":"phoneCountryCode",
        ' ',
        transaction_details:"_internalTransactionDetails":"phoneNumber"
      )

      WHEN transfer_type IN ('AIRTIME_PURCHASES_COMPLETED')
      THEN concat(
        'Bought airtime for ',
        transaction_details:"_internalTransactionDetails":"phoneCarrier",
        ' ',
        transaction_details:"_internalTransactionDetails":"phoneCountryCode",
        ' ',
        transaction_details:"_internalTransactionDetails":"phoneNumber"
      ) end as shortened_transaction_details,
        outgoing_user_id,
        incoming_user_id
    from all_transfer_ids
     
),

reverse_transfer_ids as (
    select
        reverse_transfer_hlo_id as hlo_id,
        reverse_transfer_hlo_table as hlo_table,
        reverse_transfer_journal_id as hlo_journal_id,
        reverse_transfer_hlo_status as hlo_status,
        reverse_transfer_hlo_created_at as hlo_created_at,
        reverse_transfer_hlo_updated_at as hlo_updated_at,
        reverse_transfer_id as transfer_id,
        is_original_transfer_reversed,
        TRUE as is_transfer_reversal,
        external_provider,
        external_provider_transaction_id,
        case
            when is_transfer_reversal = TRUE then 'AIRTIME_PURCHASES_REVERSAL'
            else concat(hlo_table, '_', hlo_status)
        end as transfer_type,
        object_construct(
        '_internalTransactionDetails',object_construct(
            'phoneCarrier', coalesce(phone_carrier, try_parse_json('NULL')),
            'phoneCountryCode', coalesce(phone_country_code,try_parse_json('NULL')),
            'phoneNumber', coalesce(phone_number,try_parse_json('NULL')),
            'discountPercentage', coalesce(discount_percentage,try_parse_json('NULL')),
            'commissionPercentage', coalesce(commission_percentage,try_parse_json('NULL')),
            'reversalForOriginalTransferId',coalesce(transfer_id,try_parse_json('NULL')),
            'reversalForOriginalJournalId',coalesce(hlo_journal_id,try_parse_json('NULL'))
        ),
        'externalProviderTransactionDetails',coalesce(provider_response,try_parse_json('NULL'))
    ) as transaction_details,
    CASE
      WHEN transfer_type IN (
        'AIRTIME_PURCHASES_REVERSAL',
        'AIRTIME_PURCHASES_QUEUED_FOR_REFUND',
        'AIRTIME_PURCHASES_PENDING',
        'AIRTIME_PURCHASES_FAILED'
      ) 
      THEN concat(
        'Attempted airtime purchase for ',
        transaction_details:"_internalTransactionDetails":"phoneCarrier",
        ' ',
        transaction_details:"_internalTransactionDetails":"phoneCountryCode",
        ' ',
        transaction_details:"_internalTransactionDetails":"phoneNumber"
      )

      WHEN transfer_type IN ('AIRTIME_PURCHASES_COMPLETED')
      THEN concat(
        'Bought airtime for ',
        transaction_details:"_internalTransactionDetails":"phoneCarrier",
        ' ',
        transaction_details:"_internalTransactionDetails":"phoneCountryCode",
        ' ',
        transaction_details:"_internalTransactionDetails":"phoneNumber"
      ) end as shortened_transaction_details,
        outgoing_user_id,
        incoming_user_id
    from all_transfer_ids
    where is_original_transfer_reversed = TRUE
)
select *
from transfer_ids
union
select *
from reverse_transfer_ids
