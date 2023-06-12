{{ config(materialized='ephemeral') }}

with deposits_webhooks_and_charges_reverse_transfer_details as (
    select 
        reverse_transfer_id as transfer_id,
        hlo_table,
        external_provider,
        external_provider_transaction_id,
        concat(hlo_table, '_', 'REVERSAL') as transfer_type, 
        object_construct(
            '_internalTransactionDetails',object_construct(
                'errorMessage', error_message,
                'note', note,
                'adminId',admin_id,
                'paymentMethodDetails' ,payment_method_details,
                'otherDepositDetails',other_deposit_details,
                'chargeDetails',charge_details,
                'cardChargeDetails',card_charge_details,
                'depositWebhooksReceiptDetails', deposit_webhooks_receipt_details,
                'bankChargeDetails', bank_charge_details
            ),
            'externalProviderTransactionDetails',external_provider_transaction_details
        ) as transaction_details,
        CASE WHEN transfer_type IN ('DEPOSITS_REVERSAL')
      AND trim(transaction_details:"_internalTransactionDetails":"paymentMethodDetails":"type",'"') = 'MOBILE_MONEY' 
      THEN concat (
        'Attempted Cash-in from mobile money: ',
        transaction_details:"_internalTransactionDetails":"mobileMoneyPaymentMethodDetails"[0]:"mobileMoneyCarrier",
        ' ',
        transaction_details:"_internalTransactionDetails":"mobileMoneyPaymentMethodDetails"[0]:"mobileMoneyAccountCountry",
        ' ',
        transaction_details:"_internalTransactionDetails":"mobileMoneyPaymentMethodDetails"[0]:"mobileMoneyAccountNumber"
      )

      WHEN transfer_type IN ('DEPOSITS_REVERSAL')
      AND trim(transaction_details:"_internalTransactionDetails":"paymentMethodDetails":"type",'"') = 'CARD' 
      THEN concat (
        'Attempted Cash-in from card: ',
        transaction_details:"_internalTransactionDetails":"paymentMethodDetails":"paymentCardPaymentMethodDetails"[0]:"paymentCardCardNetwork",
        ' ',
        transaction_details:"_internalTransactionDetails":"paymentMethodDetails":"paymentCardPaymentMethodDetails"[0]:"paymentCardLastFour",
        ' issued by BIN ',
        transaction_details:"_internalTransactionDetails":"paymentMethodDetails":"paymentCardPaymentMethodDetails"[0]:"paymentCardBin",
        ' - ',
        transaction_details:"_internalTransactionDetails":"paymentMethodDetails":"paymentCardPaymentMethodDetails"[0]:"paymentCardIssuingBank"
      )

      WHEN  transfer_type IN ('DEPOSITS_REVERSAL')
      AND trim(transaction_details:"_internalTransactionDetails":"paymentMethodDetails":"type",'"') = 'BANK' 
      THEN concat (
        'Attempted Cash-in from bank account: ',
        COALESCE(transaction_details:"_internalTransactionDetails":"paymentMethodDetails":"bankAccountPaymentMethodDetails"[0]:"bankName",'railsbank'),
        ' ',
        COALESCE(transaction_details:"_internalTransactionDetails":"paymentMethodDetails":"bankAccountPaymentMethodDetails"[0]:"bankAccountNumber",transaction_details:"_internalTransactionDetails":"paymentMethodDetails":"railsbankPaymentMethodDetails"[0]:"railsbankUkAccountNumber")
      )

      WHEN transfer_type IN ('DEPOSITS_REVERSAL')
      AND trim(transaction_details:"_internalTransactionDetails":"paymentMethodDetails":"type",'"') = 'NUBAN'
      THEN concat (
        'Attempted Cash-in from Chipper Account Number: ',
        transaction_details:"_internalTransactionDetails":"paymentMethodDetails":"nubanPaymentMethodDetails"[0]:"nubanAccountName",
        ' ',
        transaction_details:"_internalTransactionDetails":"paymentMethodDetails":"nubanPaymentMethodDetails"[0]:"nubanAccountNumber",
        ' with ',
        transaction_details:"_internalTransactionDetails":"paymentMethodDetails":"nubanPaymentMethodDetails"[0]:"bankname",
        ' with note: ',
        transaction_details:"_internalTransactionDetails":"note"
      ) end as shortened_transaction_details,
        created_at as hlo_created_at,
        updated_at as hlo_updated_at,
        status as hlo_status,
        user_id as outgoing_user_id,
        null as incoming_user_id
    from {{ref('deposits_with_webhooks_and_charges')}}
    where reverse_transfer_id is not null
),dead_end_reverse_transfer_details as (
    select 
        reverse_transfer_id as transfer_id,
        hlo_table,
        external_provider,
        external_provider_transaction_id, 
        concat(hlo_table, '_', 'REVERSAL') as transfer_type,
        object_construct(
            '_internalTransactionDetails',object_construct(
                'errorMessage', error_message,
                'note', note,
                'adminId',admin_id,
                'paymentMethodDetails' ,payment_method_details,
                'otherDepositDetails',other_deposit_details,
                'chargeDetails',charge_details,
                'cardChargeDetails',card_charge_details,
                'depositWebhooksReceiptDetails', deposit_webhooks_receipt_details,
                'bankChargeDetails', bank_charge_details
            ),
            'externalProviderTransactionDetails',external_provider_transaction_details
        ) as transaction_details,
        CASE WHEN transfer_type IN ('DEPOSITS_REVERSAL')
      AND trim(transaction_details:"_internalTransactionDetails":"paymentMethodDetails":"type",'"') = 'MOBILE_MONEY' 
      THEN concat (
        'Attempted Cash-in from mobile money: ',
        transaction_details:"_internalTransactionDetails":"mobileMoneyPaymentMethodDetails"[0]:"mobileMoneyCarrier",
        ' ',
        transaction_details:"_internalTransactionDetails":"mobileMoneyPaymentMethodDetails"[0]:"mobileMoneyAccountCountry",
        ' ',
        transaction_details:"_internalTransactionDetails":"mobileMoneyPaymentMethodDetails"[0]:"mobileMoneyAccountNumber"
      )

      WHEN transfer_type IN ('DEPOSITS_REVERSAL')
      AND trim(transaction_details:"_internalTransactionDetails":"paymentMethodDetails":"type",'"') = 'CARD' 
      THEN concat (
        'Attempted Cash-in from card: ',
        transaction_details:"_internalTransactionDetails":"paymentMethodDetails":"paymentCardPaymentMethodDetails"[0]:"paymentCardCardNetwork",
        ' ',
        transaction_details:"_internalTransactionDetails":"paymentMethodDetails":"paymentCardPaymentMethodDetails"[0]:"paymentCardLastFour",
        ' issued by BIN ',
        transaction_details:"_internalTransactionDetails":"paymentMethodDetails":"paymentCardPaymentMethodDetails"[0]:"paymentCardBin",
        ' - ',
        transaction_details:"_internalTransactionDetails":"paymentMethodDetails":"paymentCardPaymentMethodDetails"[0]:"paymentCardIssuingBank"
      )

      WHEN transfer_type IN ('DEPOSITS_REVERSAL')
      AND trim(transaction_details:"_internalTransactionDetails":"paymentMethodDetails":"type",'"') = 'BANK' 
      THEN concat (
        'Attempted Cash-in from bank account: ',
        COALESCE(transaction_details:"_internalTransactionDetails":"paymentMethodDetails":"bankAccountPaymentMethodDetails"[0]:"bankName",'railsbank'),
        ' ',
        COALESCE(transaction_details:"_internalTransactionDetails":"paymentMethodDetails":"bankAccountPaymentMethodDetails"[0]:"bankAccountNumber",transaction_details:"_internalTransactionDetails":"paymentMethodDetails":"railsbankPaymentMethodDetails"[0]:"railsbankUkAccountNumber")
      )

      WHEN transfer_type IN ('DEPOSITS_REVERSAL')
      AND trim(transaction_details:"_internalTransactionDetails":"paymentMethodDetails":"type",'"') = 'NUBAN'
      THEN concat (
        'Attempted Cash-in from Chipper Account Number: ',
        transaction_details:"_internalTransactionDetails":"paymentMethodDetails":"nubanPaymentMethodDetails"[0]:"nubanAccountName",
        ' ',
        transaction_details:"_internalTransactionDetails":"paymentMethodDetails":"nubanPaymentMethodDetails"[0]:"nubanAccountNumber",
        ' with ',
        transaction_details:"_internalTransactionDetails":"paymentMethodDetails":"nubanPaymentMethodDetails"[0]:"bankname",
        ' with note: ',
        transaction_details:"_internalTransactionDetails":"note"
      ) end as shortened_transaction_details,
        created_at as hlo_created_at,
        updated_at as hlo_updated_at,
        status as hlo_status,
        user_id as outgoing_user_id,
        null as incoming_user_id
    from {{ref('dead_end_deposits')}}
    where reverse_transfer_id is not null
)

select 
    * 
from deposits_webhooks_and_charges_reverse_transfer_details
union 
select 
    * 
from dead_end_reverse_transfer_details
