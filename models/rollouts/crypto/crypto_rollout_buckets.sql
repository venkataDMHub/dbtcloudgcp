WITH DEPOSITORS AS (
  SELECT DISTINCT USER_ID 
  FROM CHIPPER.{{ var("core_public") }}.CRYPTO_DEPOSITS
  WHERE created_at >= '2021-06-01'
),

WITHDRAWERS AS (
  SELECT DISTINCT USER_ID
    FROM CHIPPER.AURORA_CORE3_PUBLIC.CRYPTO_WITHDRAWALS
    WHERE CREATED_AT >= '2021-06-01'
),

TRANSACTORS AS (
  SELECT MAIN_PARTY_USER_ID, COUNTER_PARTY_USER_ID 
  FROM {{ ref('expanded_ledgers') }}
  WHERE CORRIDOR = 'CRYPTO_TRADE'
  AND HLO_CREATED_AT >= '2021-06-01'
),

RELEVANT_CRYPTO_USERS AS
(
  SELECT USER_ID FROM DEPOSITORS
  UNION
  SELECT MAIN_PARTY_USER_ID AS USER_ID FROM TRANSACTORS
  UNION
  SELECT COUNTER_PARTY_USER_ID AS USER_ID FROM TRANSACTORS
  UNION
  SELECT USER_ID FROM WITHDRAWERS
),

CRYPTO_ROLLOUT_SEGMENT AS
(
  SELECT RELEVANT_CRYPTO_USERS.*, USERS.PRIMARY_CURRENCY, (ABS(MOD(HASH(USER_ID), 100)) + 1) / 100 AS PERCENTILE
  FROM RELEVANT_CRYPTO_USERS
  JOIN CHIPPER.{{ var("core_public") }}.USERS
  ON RELEVANT_CRYPTO_USERS.USER_ID = USERS.ID
)

SELECT USER_ID, PRIMARY_CURRENCY, PERCENTILE
FROM CRYPTO_ROLLOUT_SEGMENT