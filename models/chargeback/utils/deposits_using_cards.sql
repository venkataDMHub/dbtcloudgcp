{{ config(materialized='view') }}

select
    transfer_id,
    hlo_table,
    external_provider,
    external_provider_transaction_id,
    transaction_details,
    null as nuban_bank_name,
    null as nuban_account_number,
    null as nuban_account_name,
    payment_card_payment_method_details.value:paymentCardBin::text as payment_card_bin,
    payment_card_payment_method_details.value:paymentCardCardNetwork::text as payment_card_card_network,
    payment_card_payment_method_details.value:paymentCardIssuingBank::text as payment_card_issuing_bank,
    payment_card_payment_method_details.value:paymentCardCardType::text as payment_card_card_type,
    payment_card_payment_method_details.value:paymentCardExpiryDate::text as payment_card_expiry_date,
    payment_card_payment_method_details.value:paymentCardLastFour::text as payment_card_last_four
from
    {{ ref('transaction_details') }},
    lateral flatten(
        input => transaction_details:_internalTransactionDetails:paymentMethodDetails:paymentCardPaymentMethodDetails
    ) as payment_card_payment_method_details
where
    hlo_table = 'DEPOSITS'
