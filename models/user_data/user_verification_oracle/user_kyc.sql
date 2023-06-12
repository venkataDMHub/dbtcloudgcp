{{
    config(
        materialized='ephemeral'
    )
}}

WITH kyc_agg AS (
    SELECT
        owner_id AS user_id,
        count(*) AS kyc_accepted_doc_count
    FROM
        {{ var("compliance_public") }}.kyc_documents
    WHERE
        status = 'ACCEPTED'
    GROUP BY
        user_id
),

latest_kyc AS (
    SELECT
        owner_id AS user_id,
        updated_at AS latest_accepted_kyc_submitted_at
    FROM
        {{ var("compliance_public") }}.kyc_documents
    WHERE
        status = 'ACCEPTED'
    QUALIFY
        row_number() OVER (PARTITION BY owner_id ORDER BY submitted_at DESC) = 1
)

SELECT
    kyc_agg.user_id,
    kyc_agg.kyc_accepted_doc_count,
    latest_kyc.latest_accepted_kyc_submitted_at
FROM
    kyc_agg
INNER JOIN latest_kyc ON kyc_agg.user_id = latest_kyc.user_id
