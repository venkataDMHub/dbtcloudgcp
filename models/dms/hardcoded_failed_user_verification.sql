{{ config(materialized = 'ephemeral') }} 

WITH hard_coded_ngn AS (
    SELECT
        'dms_user_ngn',
        'FAILED',
        DATE_TRUNC('HOUR', CURRENT_TIMESTAMP()),
        'NGN'
),
hard_coded_ugx AS (
    SELECT
        'dms_user_ugx',
        'FAILED',
        DATE_TRUNC('HOUR', CURRENT_TIMESTAMP()),
        'UGX'
),
hard_coded_ghs AS (
    SELECT
        'dms_user_ghs',
        'FAILED',
        DATE_TRUNC('HOUR', CURRENT_TIMESTAMP()),
        'GHS'
)
SELECT
    *
FROM
    hard_coded_ngn
UNION
SELECT
    *
FROM
    hard_coded_ugx
UNION
SELECT
    *
FROM
    hard_coded_ghs
