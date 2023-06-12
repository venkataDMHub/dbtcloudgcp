{{  config(
        materialized='incremental',
        unique_key='transfer_id',
        on_schema_change='append_new_columns', 
        schema='intermediate') }}

with all_transfer_ids as (
    select
        withdrawals.id::text as hlo_id,
        'WITHDRAWALS' as hlo_table,
        withdrawals.journal_id as hlo_journal_id,
        withdrawals.status as hlo_status,
        withdrawals.created_at as hlo_created_at,
        withdrawals.updated_at as hlo_updated_at,
        withdrawals.transfer_id as transfer_id,
        withdrawals.reverse_transfer_id as reverse_transfer_id,
        case when withdrawals.reverse_transfer_id is null then false
            else true
        end as is_original_transfer_reversed,
        withdrawals.is_s2nc, 
        withdrawals.provider as external_provider,
        withdrawals.provider_id as external_provider_transaction_id,
        payment_methods.payment_method_details as payment_method_details,
        withdrawals.admin_id,
        withdrawals.network_id,
        withdrawals.error_message,
        withdrawals.details,
        payments.note as payments_note,
        withdrawals.linked_account_id,
    case when withdrawals.reverse_transfer_id is null then withdrawals.user_id else null end as outgoing_user_id,
    case when withdrawals.reverse_transfer_id is not null then withdrawals.user_id else null end as incoming_user_id
    from
        "CHIPPER".{{ var("core_public") }}."WITHDRAWALS"
        left join {{ ref('payment_methods') }} as payment_methods
        on withdrawals.linked_account_id = payment_methods.linked_account_id
        left join "CHIPPER".{{ var("core_public") }}."PAYMENTS" as payments 
        on withdrawals.reverse_transfer_id = payments.transfer_id

    where withdrawals.transfer_id is not null
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
        is_s2nc,
        false as is_transfer_reversal,
        external_provider,
        external_provider_transaction_id,
        linked_account_id,
        case
            when
                is_transfer_reversal = true then concat('WITHDRAWALS', '_', 'REVERSAL')
            else concat(hlo_table, '_', hlo_status)
        end as transfer_type,
        object_construct(
        '_internalTransactionDetails',object_construct(
            'paymentMethodDetails',payment_method_details,
            'adminId',coalesce(all_transfer_ids.admin_id,try_parse_json('NULL')),
            'networkId',coalesce(network_id,try_parse_json('NULL')),
            'errorMessage',coalesce(error_message,try_parse_json('NULL')),
            'originalTransferForReverseTransferId',coalesce(all_transfer_ids.reverse_transfer_id,try_parse_json('NULL'))                  
        ),     
        'externalProviderTransactionDetails',details
        ) as transaction_details,
        incoming_user_id,
        outgoing_user_id
    from
        all_transfer_ids
),

reverse_transfer_ids as (

    select
        payments.id::text as hlo_id,
        'PAYMENTS' as hlo_table,
        payments.journal_id as hlo_journal_id,
        payments.status as hlo_status,
        payments.created_at as hlo_created_at,
        payments.updated_at as hlo_updated_at,
        all_transfer_ids.reverse_transfer_id as transfer_id,
        is_original_transfer_reversed,
        is_s2nc,
        true as is_transfer_reversal,
        external_provider,
        external_provider_transaction_id,
        linked_account_id,
        case
            when
                is_transfer_reversal = true then concat('WITHDRAWALS', '_', 'REVERSAL')
            else concat(hlo_table, '_', hlo_status)
        end as transfer_type,
        object_construct(
        '_internalTransactionDetails',object_construct(
            'paymentMethodDetails',payment_method_details,
            'paymentNote',coalesce(payments_note,try_parse_json('NULL')),
            'adminId',coalesce(all_transfer_ids.admin_id,try_parse_json('NULL')),
            'networkId',coalesce(network_id,try_parse_json('NULL')),
            'errorMessage',coalesce(all_transfer_ids.error_message,try_parse_json('NULL')),
            'reversalForOriginalTransferId',coalesce(all_transfer_ids.transfer_id,try_parse_json('NULL')),
            'reversalForOriginalJournalId',coalesce(hlo_journal_id,try_parse_json('NULL'))
        ),
        'externalProviderTransactionDetails',details
        ) as transaction_details,
        incoming_user_id,
        outgoing_user_id

    from all_transfer_ids
    inner join "CHIPPER".{{ var("core_public") }}."PAYMENTS"
               on all_transfer_ids.reverse_transfer_id = payments.transfer_id
    where is_original_transfer_reversed = true
)
(select transfer_ids.*,
        CASE WHEN transfer_type IN ('WITHDRAWALS_SETTLED')
        AND trim(transaction_details:"_internalTransactionDetails":"paymentMethodDetails":"type",'"') = 'BANK' 
        THEN concat (
            'Cash-out to bank account: ',
            transaction_details:"_internalTransactionDetails":"paymentMethodDetails":"bankAccountPaymentMethodDetails"[0]:"bankName",
            ' ',
            transaction_details:"_internalTransactionDetails":"paymentMethodDetails":"bankAccountPaymentMethodDetails"[0]:"bankAccountNumber",' ',
            COALESCE(transaction_details:"externalProviderTransactionDetails":"settled":"transfer":"fullname",'no_data'),' ',
            COALESCE(transaction_details:"externalProviderTransactionDetails":"settled":"transfer":"currency",'no_data')

        )

        WHEN transfer_type IN ('WITHDRAWALS_SETTLED')
        AND trim(transaction_details:"_internalTransactionDetails":"paymentMethodDetails":"type",'"') = 'MOBILE_MONEY' 
        THEN concat (
            'Cash-out to mobile money: ',
            transaction_details:"_internalTransactionDetails":"paymentMethodDetails":"mobileMoneyPaymentMethodDetails"[0]:"mobileMoneyCarrier",
            ' ',
            transaction_details:"_internalTransactionDetails":"paymentMethodDetails":"mobileMoneyPaymentMethodDetails"[0]:"mobileMoneyCountry",
            ' ',
            transaction_details:"_internalTransactionDetails":"paymentMethodDetails":"mobileMoneyPaymentMethodDetails"[0]:"mobileMoneyPhone",' ',
            COALESCE(concat(MOBILE_MONEY.First_Name,' ',MOBILE_MONEY.LAST_NAME),'no_data')
        )

        WHEN transfer_type IN ('WITHDRAWALS_SETTLED') 
        THEN 'Cash-out'

        WHEN transfer_type IN (
            'WITHDRAWALS_QUEUED_FOR_REFUND',
            'WITHDRAWALS_PENDING',
            'WITHDRAWALS_EXTERNAL_FAILED',
            'WITHDRAWALS_QUEUED',
            'WITHDRAWALS_WAITING_ON_SCREENING',
            'WITHDRAWALS_FAILED',
            'WITHDRAWALS_PAYMENT_SUBMITTED',
            'WITHDRAWALS_WAITING_APPROVAL',
            'WITHDRAWALS_REVERSAL'
        )
        AND trim(transaction_details:"_internalTransactionDetails":"paymentMethodDetails":"type",'"') = 'BANK'
        THEN concat (
            'Attempted cash-out to bank account: ',
            transaction_details:"_internalTransactionDetails":"paymentMethodDetails":"bankAccountPaymentMethodDetails"[0]:"bankName",
            ' ',
            transaction_details:"_internalTransactionDetails":"paymentMethodDetails":"bankAccountPaymentMethodDetails"[0]:"bankAccountNumber"
        )

        WHEN transfer_type IN (
            'WITHDRAWALS_QUEUED_FOR_REFUND',
            'WITHDRAWALS_PENDING',
            'WITHDRAWALS_EXTERNAL_FAILED',
            'WITHDRAWALS_QUEUED',
            'WITHDRAWALS_WAITING_ON_SCREENING',
            'WITHDRAWALS_REVERSAL',
            'WITHDRAWALS_FAILED',
            'WITHDRAWALS_PAYMENT_SUBMITTED',
            'WITHDRAWALS_WAITING_APPROVAL'
        )
        AND trim(transaction_details:"_internalTransactionDetails":"paymentMethodDetails":"type",'"') = 'MOBILE_MONEY'
        THEN concat (
            'Attempted cash-out to mobile money: ',
            transaction_details:"_internalTransactionDetails":"paymentMethodDetails":"mobileMoneyPaymentMethodDetails"[0]:"mobileMoneyCarrier",
            ' ',
            transaction_details:"_internalTransactionDetails":"paymentMethodDetails":"mobileMoneyPaymentMethodDetails"[0]:"mobileMoneyCountry",
            ' ',
            transaction_details:"_internalTransactionDetails":"paymentMethodDetails":"mobileMoneyPaymentMethodDetails"[0]:"mobileMoneyPhone"
        )

        WHEN transfer_type IN (
            'WITHDRAWALS_QUEUED_FOR_REFUND',
            'WITHDRAWALS_PENDING',
            'WITHDRAWALS_EXTERNAL_FAILED',
            'WITHDRAWALS_QUEUED',
            'WITHDRAWALS_WAITING_ON_SCREENING',
            'WITHDRAWALS_FAILED',
            'WITHDRAWALS_PAYMENT_SUBMITTED',
            'WITHDRAWALS_WAITING_APPROVAL',
            'WITHDRAWALS_REVERSAL'
        )
        AND trim(transaction_details:"_internalTransactionDetails":"paymentMethodDetails":"type",'"') = 'CARD'
        THEN concat (
            'Attempted cash-out to card: ',
            transaction_details:"_internalTransactionDetails":"paymentMethodDetails":"paymentCardPaymentMethodDetails"[0]:"paymentCardCardNetwork",
            ' ',
            transaction_details:"_internalTransactionDetails":"paymentMethodDetails":"paymentCardPaymentMethodDetails"[0]:"paymentCardLastFour",
            ' issued by BIN',
            transaction_details:"_internalTransactionDetails":"paymentMethodDetails":"paymentCardPaymentMethodDetails"[0]:"paymentCardBin",
            ' - ',
            transaction_details:"_internalTransactionDetails":"paymentMethodDetails":"paymentCardPaymentMethodDetails"[0]:"paymentCardIssuingBank",
            ' ',
            transaction_details:"_internalTransactionDetails":"paymentMethodDetails":"paymentCardPaymentMethodDetails"[0]:"paymentCardCardType"
        )

        WHEN transfer_type IN (
            'WITHDRAWALS_QUEUED_FOR_REFUND',
            'WITHDRAWALS_PENDING',
            'WITHDRAWALS_EXTERNAL_FAILED',
            'WITHDRAWALS_QUEUED',
            'WITHDRAWALS_WAITING_ON_SCREENING',
            'WITHDRAWALS_FAILED',
            'WITHDRAWALS_PAYMENT_SUBMITTED',
            'WITHDRAWALS_WAITING_APPROVAL',
            'WITHDRAWALS_REVERSAL'
        )
        AND transaction_details:"_internalTransactionDetails":"paymentMethodDetails":"type" IS NULL
        THEN transaction_details:"_internalTransactionDetails":"errorMessage"
        END AS shortened_transaction_details
from
    transfer_ids
    left join "CHIPPER".{{ var("core_public") }}."MOBILE_MONEY" as MOBILE_MONEY
    ON transfer_ids.linked_account_id =
    MOBILE_MONEY.linked_account_id)
union
(select reverse_transfer_ids.*,
        CASE WHEN transfer_type IN ('WITHDRAWALS_SETTLED')
        AND trim(transaction_details:"_internalTransactionDetails":"paymentMethodDetails":"type",'"') = 'BANK' 
        THEN concat (
            'Cash-out to bank account: ',
            transaction_details:"_internalTransactionDetails":"paymentMethodDetails":"bankAccountPaymentMethodDetails"[0]:"bankName",
            ' ',
            transaction_details:"_internalTransactionDetails":"paymentMethodDetails":"bankAccountPaymentMethodDetails"[0]:"bankAccountNumber",' ',
            COALESCE(transaction_details:"externalProviderTransactionDetails":"settled":"transfer":"fullname",'no_data'),' ',
            COALESCE(transaction_details:"externalProviderTransactionDetails":"settled":"transfer":"currency",'no_data')

        )

        WHEN transfer_type IN ('WITHDRAWALS_SETTLED')
        AND trim(transaction_details:"_internalTransactionDetails":"paymentMethodDetails":"type",'"') = 'MOBILE_MONEY' 
        THEN concat (
            'Cash-out to mobile money: ',
            transaction_details:"_internalTransactionDetails":"paymentMethodDetails":"mobileMoneyPaymentMethodDetails"[0]:"mobileMoneyCarrier",
            ' ',
            transaction_details:"_internalTransactionDetails":"paymentMethodDetails":"mobileMoneyPaymentMethodDetails"[0]:"mobileMoneyCountry",
            ' ',
            transaction_details:"_internalTransactionDetails":"paymentMethodDetails":"mobileMoneyPaymentMethodDetails"[0]:"mobileMoneyPhone",' ',
            COALESCE(concat(MOBILE_MONEY.FIRST_Name,' ',MOBILE_MONEY.LAST_NAME),'no_data')
        )

        WHEN transfer_type IN ('WITHDRAWALS_SETTLED') 
        THEN 'Cash-out'

        WHEN transfer_type IN (
            'WITHDRAWALS_QUEUED_FOR_REFUND',
            'WITHDRAWALS_PENDING',
            'WITHDRAWALS_EXTERNAL_FAILED',
            'WITHDRAWALS_QUEUED',
            'WITHDRAWALS_WAITING_ON_SCREENING',
            'WITHDRAWALS_FAILED',
            'WITHDRAWALS_PAYMENT_SUBMITTED',
            'WITHDRAWALS_WAITING_APPROVAL',
            'WITHDRAWALS_REVERSAL'
        )
        AND trim(transaction_details:"_internalTransactionDetails":"paymentMethodDetails":"type",'"') = 'BANK'
        THEN concat (
            'Attempted cash-out to bank account: ',
            transaction_details:"_internalTransactionDetails":"paymentMethodDetails":"bankAccountPaymentMethodDetails"[0]:"bankName",
            ' ',
            transaction_details:"_internalTransactionDetails":"paymentMethodDetails":"bankAccountPaymentMethodDetails"[0]:"bankAccountNumber"
        )

        WHEN transfer_type IN (
            'WITHDRAWALS_QUEUED_FOR_REFUND',
            'WITHDRAWALS_PENDING',
            'WITHDRAWALS_EXTERNAL_FAILED',
            'WITHDRAWALS_QUEUED',
            'WITHDRAWALS_WAITING_ON_SCREENING',
            'WITHDRAWALS_REVERSAL',
            'WITHDRAWALS_FAILED',
            'WITHDRAWALS_PAYMENT_SUBMITTED',
            'WITHDRAWALS_WAITING_APPROVAL'
        )
        AND trim(transaction_details:"_internalTransactionDetails":"paymentMethodDetails":"type",'"') = 'MOBILE_MONEY'
        THEN concat (
            'Attempted cash-out to mobile money: ',
            transaction_details:"_internalTransactionDetails":"paymentMethodDetails":"mobileMoneyPaymentMethodDetails"[0]:"mobileMoneyCarrier",
            ' ',
            transaction_details:"_internalTransactionDetails":"paymentMethodDetails":"mobileMoneyPaymentMethodDetails"[0]:"mobileMoneyCountry",
            ' ',
            transaction_details:"_internalTransactionDetails":"paymentMethodDetails":"mobileMoneyPaymentMethodDetails"[0]:"mobileMoneyPhone"
        )

        WHEN transfer_type IN (
            'WITHDRAWALS_QUEUED_FOR_REFUND',
            'WITHDRAWALS_PENDING',
            'WITHDRAWALS_EXTERNAL_FAILED',
            'WITHDRAWALS_QUEUED',
            'WITHDRAWALS_WAITING_ON_SCREENING',
            'WITHDRAWALS_FAILED',
            'WITHDRAWALS_PAYMENT_SUBMITTED',
            'WITHDRAWALS_WAITING_APPROVAL',
            'WITHDRAWALS_REVERSAL'
        )
        AND trim(transaction_details:"_internalTransactionDetails":"paymentMethodDetails":"type",'"') = 'CARD'
        THEN concat (
            'Attempted cash-out to card: ',
            transaction_details:"_internalTransactionDetails":"paymentMethodDetails":"paymentCardPaymentMethodDetails"[0]:"paymentCardCardNetwork",
            ' ',
            transaction_details:"_internalTransactionDetails":"paymentMethodDetails":"paymentCardPaymentMethodDetails"[0]:"paymentCardLastFour",
            ' issued by BIN',
            transaction_details:"_internalTransactionDetails":"paymentMethodDetails":"paymentCardPaymentMethodDetails"[0]:"paymentCardBin",
            ' - ',
            transaction_details:"_internalTransactionDetails":"paymentMethodDetails":"paymentCardPaymentMethodDetails"[0]:"paymentCardIssuingBank",
            ' ',
            transaction_details:"_internalTransactionDetails":"paymentMethodDetails":"paymentCardPaymentMethodDetails"[0]:"paymentCardCardType"
        )

        WHEN transfer_type IN (
            'WITHDRAWALS_QUEUED_FOR_REFUND',
            'WITHDRAWALS_PENDING',
            'WITHDRAWALS_EXTERNAL_FAILED',
            'WITHDRAWALS_QUEUED',
            'WITHDRAWALS_WAITING_ON_SCREENING',
            'WITHDRAWALS_FAILED',
            'WITHDRAWALS_PAYMENT_SUBMITTED',
            'WITHDRAWALS_WAITING_APPROVAL',
            'WITHDRAWALS_REVERSAL'
        )
        AND transaction_details:"_internalTransactionDetails":"paymentMethodDetails":"type" IS NULL
        THEN transaction_details:"_internalTransactionDetails":"errorMessage"
        END AS shortened_transaction_details
from
    reverse_transfer_ids
    left join "CHIPPER".{{ var("core_public") }}."MOBILE_MONEY" as MOBILE_MONEY
    ON reverse_transfer_ids.linked_account_id = 
    MOBILE_MONEY.linked_account_id

)
