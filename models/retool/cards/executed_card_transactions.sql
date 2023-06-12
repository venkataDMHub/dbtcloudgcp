SELECT *
FROM {{ ref('issued_card_transactions_with_usd') }}
WHERE type IN ('FUNDING', 'WITHDRAWAL')
AND transfer_id IN (
    SELECT DISTINCT id 
    FROM {{var("core_public")}}.transfers 
    WHERE status = 'SETTLED'
    )        
UNION
SELECT *
FROM {{ ref('issued_card_transactions_with_usd') }}
WHERE type = 'TRANSACTION' 
AND status = 'COMPLETED'
