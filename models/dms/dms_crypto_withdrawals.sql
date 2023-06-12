SELECT
    crypto_withdrawals.created_at AS created_at,
    crypto_withdrawals.status AS status,
    crypto_withdrawals.asset AS asset,
    crypto_withdrawals.provider AS provider,
    crypto_withdrawals.last_error AS last_error
FROM
    "CHIPPER".{{var("core_public")}}."CRYPTO_WITHDRAWALS" AS crypto_withdrawals
