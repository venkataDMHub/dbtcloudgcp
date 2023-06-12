SELECT
    withdrawals.created_at AS created_at,
    withdrawals.status AS STATUS,
    withdrawals.provider AS provider,
    withdrawals.error_message AS error_message,
    transfers.destination_currency AS currency
FROM
    "CHIPPER".{{var("core_public")}}."WITHDRAWALS" AS withdrawals
    JOIN "CHIPPER".{{var("core_public")}}."TRANSFERS" AS transfers ON withdrawals.transfer_id = transfers.id
