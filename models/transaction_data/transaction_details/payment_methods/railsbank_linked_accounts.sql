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
        'railsbankPaymentMethodDetails', array_agg(
            object_construct(
                'railsbankUkAccountNumber', UK_ACCOUNT_NUMBER,
                'railsbankAccountName',ACCOUNT_NAME,
                'railsbankIban', IBAN,
                'railsbankBicSwift', BIC_SWIFT,
                'railsbankUkSortCode', UK_SORT_CODE,
                'railsbankCurrency',CURRENCY
            )
        ) within group (order by linked_account_id)
    ) as payment_method_details
from "CHIPPER".{{ var("core_public") }}."LINKED_ACCOUNTS"
join "CHIPPER".{{ var("core_public") }}."RAILSBANK_USER_DETAILS" 
     on linked_accounts.id = railsbank_user_details.linked_account_id

where railsbank_user_details.linked_account_id not in 
    (   
        select 
            linked_account_id 
        from {{ ref('bank_account_linked_accounts') }}
    )
group by linked_account_id,
         type,
         is_linked,
         is_external,
         is_verified,
         linked_accounts.id,
         linked_accounts.user_id,
         screening_status,
         linked_accounts.created_at
