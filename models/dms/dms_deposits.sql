SELECT
    deposits.created_at AS created_at,
    deposits.status AS STATUS,
    transfers.origin_currency AS origin_currency,
    deposits.error_message AS error_message,
    CASE
      WHEN CARD_CHARGE_ID IS NOT NULL THEN 'Debit Card Deposit'
      WHEN CHARGE_ID IS NOT NULL THEN 'Mobile Money Deposit'
      WHEN DEPOSIT_RECEIPT_WEBHOOK_ID IS NOT NULL THEN 'Nuban Deposit'
      ELSE 'Other Deposit'
    END AS deposit_type
    
FROM
    CHIPPER.{{var("core_public")}}.DEPOSITS AS deposits
    JOIN CHIPPER.{{var("core_public")}}.TRANSFERS AS transfers ON deposits.transfer_id = transfers.id
