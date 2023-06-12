{{ config(materialized='view') }}

select
    transfer_id,
    hlo_table,
    external_provider,
    external_provider_transaction_id,
    transaction_details,
    null as payment_card_bin,
    null as payment_card_card_network,
    null as payment_card_issuing_bank,
    null as payment_card_card_type,
    null as payment_card_expiry_date,
    null as payment_card_last_four,
    nuban_payment_method_details.value:nubanBankName::text as nuban_bank_name,
    nuban_payment_method_details.value:nubanAccountNumber::text as nuban_account_number,
    nuban_payment_method_details.value:nubanAccountName::text as nuban_account_name
from
    {{ ref('transaction_details') }},
    lateral flatten(
        input => transaction_details:_internalTransactionDetails:paymentMethodDetails:nubanPaymentMethodDetails
    ) as nuban_payment_method_details
where
    hlo_table = 'DEPOSITS'
