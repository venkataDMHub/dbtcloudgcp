{{ config(materialized='ephemeral') }}

select 
    linked_account_id,
    linked_accounts.user_id,
    object_construct(
        'type', type,
        'isLinked', is_linked,
        'isVerified', is_verified,
        'isExternal', is_external,
        'linkedAccountId', linked_accounts.id,
        'screeningStatus', screening_status,
        'paymentMethodCreatedAt', linked_accounts.created_at,
        'bankAccountPaymentMethodDetails', array_agg(
            object_construct(
                'bankAccountNumber', ACCOUNT_NUMBER,
                'bankAccountName',ACCOUNT_NAME,
                'bankID', BANK_ID,
                'bankAccountType', ACCOUNT_TYPE,
                'bankAccountSubType', ACCOUNT_SUBTYPE,
                'bankCurrency',CURRENCY,
                'bankRoutingNumber',ROUTING_NUMBER,
                'bankName',banks.NAME,
                'bankCountry',banks.COUNTRY,
                'bankAccountBicSwift',bank_accounts.BIC_SWIFT,
                'bankAccountSortCode',bank_accounts.SORT_CODE,
                'bankAccountIban',bank_accounts.IBAN
            )
        ) within group (order by linked_account_id)
    ) as payment_method_details
from "CHIPPER".{{ var("core_public") }}."LINKED_ACCOUNTS"
join "CHIPPER".{{ var("core_public") }}."BANK_ACCOUNTS"
     on linked_accounts.id = bank_accounts.linked_account_id
left join "CHIPPER".{{ var("core_public") }}."BANKS"
     on bank_accounts.bank_id = banks.id
group by linked_account_id,
         type,
         is_linked,
         is_external,
         is_verified,
         linked_accounts.id,
         linked_accounts.user_id,
         screening_status,
         linked_accounts.created_at

