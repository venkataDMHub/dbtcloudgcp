SELECT
    crypto_deposits.created_at AS created_at,
    crypto_deposits.status AS status,
    crypto_deposits.asset AS asset,
    crypto_deposits.provider AS provider,
    crypto_deposits.last_error AS last_error
FROM
    "CHIPPER".{{var("core_public")}}."CRYPTO_DEPOSITS" AS crypto_deposits
