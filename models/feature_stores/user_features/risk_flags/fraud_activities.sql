{{ config(materialized='view') }}

SELECT
    user_id,
    'TRANSACTION_FRAUD_ACTIVITIES' AS risk_type,
    CASE 
        WHEN (is_card_chargeback = TRUE AND is_bank_chargeback = FALSE) THEN 'CHARGEBACK_FRAUD' 
        WHEN (is_card_chargeback = FALSE AND is_bank_chargeback = TRUE) THEN 'NUBAN_REVERSAL_FRAUD' 
        ELSE 'NONE'
    END AS risk_reason
FROM CHIPPER.DBT_TRANSFORMATIONS.EXPANDED_CHARGEBACKS
WHERE risk_reason <> 'NONE' AND
      user_id IS NOT NULL
GROUP BY user_id, risk_type, risk_reason
