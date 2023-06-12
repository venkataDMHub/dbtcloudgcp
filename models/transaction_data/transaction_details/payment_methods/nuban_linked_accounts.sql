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
        'nubanPaymentMethodDetails',array_agg(
            object_construct(
                    'nubanAccountNumber', ACCOUNT_NUMBER,
                    'nubanIsActive', IS_ACTIVE,
                    'nubanProvider', PROVIDER,
                    'nubanAccountName', ACCOUNT_NAME,
                    'nubanBankName',BANK_NAME,
                    'nubanDetails',DETAILS,
                    'nubanProviderStatus',PROVIDER_STATUS
            )
        ) within group (order by linked_account_id)
    ) as payment_method_details
from "CHIPPER".{{ var("core_public") }}."LINKED_ACCOUNTS"
join "CHIPPER".{{ var("core_public") }}."NUBANS"
     on linked_accounts.id = nubans.linked_account_id
group by linked_account_id,
         type,
         is_linked,
         is_external,
         is_verified,
         linked_accounts.id,
         linked_accounts.user_id,
         screening_status,
         linked_accounts.created_at
