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
        'paymentCardPaymentMethodDetails', array_agg(
            object_construct(
                'paymentCardCardNetwork', CARD_NETWORK,
                'paymentCardBin', BIN,
                'paymentCardCardType', CARD_TYPE,
                'paymentCardIssuingBank', ISSUING_BANK,
                'paymentCardExpiryDate',EXPIRY_DATE,
                'paymentCardLastFour',LAST_FOUR,
                'paymentCardAuthTokenIssuedBy',AUTH_TOKEN_ISSUED_BY,
                'paymentCardAuthToken',AUTH_TOKEN,
                'paymentCardIsReusable',REUSABLE,
                'paymentCardIsValid',IS_VALID
            )
        ) within group (order by linked_account_id)
    ) as payment_method_details
from "CHIPPER".{{ var("core_public") }}."LINKED_ACCOUNTS"
join "CHIPPER".{{ var("core_public") }}."PAYMENT_CARDS"
     on linked_accounts.id = payment_cards.linked_account_id
group by linked_account_id,
         type,
         is_linked,
         is_external,
         is_verified,
         linked_accounts.id,
         linked_accounts.user_id,
         screening_status,
         linked_accounts.created_at
