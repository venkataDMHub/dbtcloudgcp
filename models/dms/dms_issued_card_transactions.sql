select
    issued_card_transactions.created_at as created_at,
    issued_card_transactions.status as status,
    issued_card_transactions.type as type,
    issued_card_transactions.currency as currency,
    issued_card_transactions.error_message as error_message, 
    issued_card_transactions.country as country
from
    "CHIPPER".{{var("core_public")}}."ISSUED_CARD_TRANSACTIONS"
