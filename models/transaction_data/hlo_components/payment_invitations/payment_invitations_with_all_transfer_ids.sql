{{  config(
        materialized='incremental',
        unique_key='transfer_id',
        on_schema_change='append_new_columns', 
        schema='intermediate') }}

with invite_hold_transfer_ids as (
    select
        id::text as hlo_id,
        'PAYMENT_INVITATIONS' as hlo_table,
        initial_journal_id as hlo_journal_id,
        status as hlo_status,
        created_at as hlo_created_at,
        updated_at as hlo_updated_at,
        initial_transfer_id as transfer_id,
        case when completed_journal_id is not null and recipient_id is null then TRUE
            when completed_transfer_id is not null and recipient_id is null then TRUE
            else FALSE
        end as is_original_transfer_reversed,
        FALSE as is_transfer_reversal,
        null as external_provider,
        null as external_provider_transaction_id,
        case
            when is_transfer_reversal = TRUE then concat(hlo_table, '_', 'REVERSAL')
            else concat(hlo_table, '_', hlo_status)
        end as transfer_type,
        object_construct(
        '_internalTransactionDetails', object_construct(
            'note', coalesce(note, try_parse_json('NULL')),
            'recipientMatcher', coalesce(recipient_matcher,try_parse_json('NULL')),
            'matcherType',coalesce(matcher_type,try_parse_json('NULL')),
            'errorMessage',coalesce(error_message,try_parse_json('NULL')),
            'inviteHoldForCompletedTransferId',coalesce(completed_transfer_id,try_parse_json('NULL')),
            'inviteHoldForCompletedJournalId',coalesce(completed_journal_id,try_parse_json('NULL'))
        ),
        'externalProviderTransactionDetails',try_parse_json('NULL')
    ) as transaction_details,
    CASE WHEN transfer_type IN (
        'PAYMENT_INVITATIONS_CANCELLED',
        'PAYMENT_INVITATIONS_PENDING',
        'PAYMENT_INVITATIONS_EXPIRED',
        'PAYMENT_INVITATIONS_FAILED',
        'PAYMENT_INVITATIONS_REVERSAL'
      )
      THEN concat (
        'Attempted payment invitation to ',
        transaction_details:"_internalTransactionDetails":"recipientMatcher"
      )

      WHEN transfer_type IN ('PAYMENT_INVITATIONS_SETTLED') 
      THEN concat (
        'Payment invitation to ',
        transaction_details:"_internalTransactionDetails":"recipientMatcher",
        ' with',
        transaction_details:"_internalTransactionDetails":"note"
      ) end as shortened_transaction_details,
    sender_id as outgoing_user_id,
    recipient_id as incoming_user_id
    from
        "CHIPPER".{{ var("core_public") }}."PAYMENT_INVITATIONS"
    where initial_transfer_id is not null
    {% if is_incremental() %}
        -- this filter will only be applied on an incremental run
        and hlo_updated_at >= (select max(hlo_updated_at) from {{ this }})
    {% endif %}
),

invite_resolve_transfer_ids as (
    select
        id::text as hlo_id,
        'PAYMENT_INVITATIONS' as hlo_table,
        completed_journal_id as hlo_journal_id,
        status as hlo_status,
        created_at as hlo_created_at,
        updated_at as hlo_updated_at,
        completed_transfer_id as transfer_id,
        case when completed_journal_id is not null and recipient_id is null then TRUE
            when completed_transfer_id is not null and recipient_id is null then TRUE
            else FALSE
        end as is_original_transfer_reversed,
        case when is_original_transfer_reversed = TRUE then TRUE
            else FALSE
        end as is_transfer_reversal,
        null as external_provider,
        null as external_provider_transaction_id,
        case
        when is_transfer_reversal = TRUE then concat(hlo_table, '_', 'REVERSAL')
        else concat(hlo_table, '_', hlo_status)
        end as transfer_type,
        object_construct(
        '_internalTransactionDetails', object_construct(
            'note', coalesce(note, try_parse_json('NULL')),
            'recipientMatcher', coalesce(recipient_matcher,try_parse_json('NULL')),
            'matcherType',coalesce(matcher_type,try_parse_json('NULL')),
            'errorMessage',coalesce(error_message,try_parse_json('NULL')),
            'inviteResolveForInitialTransferId',coalesce(initial_transfer_id,try_parse_json('NULL')),
            'inviteResolveForInitialJournalId',coalesce(initial_journal_id,try_parse_json('NULL'))
        ),
        'externalProviderTransactionDetails',try_parse_json('NULL')
    ) as transaction_details,
        CASE WHEN transfer_type IN (
        'PAYMENT_INVITATIONS_CANCELLED',
        'PAYMENT_INVITATIONS_PENDING',
        'PAYMENT_INVITATIONS_EXPIRED',
        'PAYMENT_INVITATIONS_FAILED',
        'PAYMENT_INVITATIONS_REVERSAL'
      )
      THEN concat (
        'Attempted payment invitation to ',
        transaction_details:"_internalTransactionDetails":"recipientMatcher"
      )

      WHEN transfer_type IN ('PAYMENT_INVITATIONS_SETTLED') 
      THEN concat (
        'Payment invitation to ',
        transaction_details:"_internalTransactionDetails":"recipientMatcher",
        ' with',
        transaction_details:"_internalTransactionDetails":"note"
      ) end as shortened_transaction_details,
    sender_id as outgoing_user_id,
    recipient_id as incoming_user_id
    from
        "CHIPPER".{{ var("core_public") }}."PAYMENT_INVITATIONS"

    where completed_transfer_id is not null

        {% if is_incremental() %}
        -- this filter will only be applied on an incremental run
        and hlo_updated_at >= (select max(hlo_updated_at) from {{ this }})
    {% endif %}
)

select * 
from invite_hold_transfer_ids

union

select * 
from invite_resolve_transfer_ids

