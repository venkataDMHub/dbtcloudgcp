{{
    config(
        materialized='ephemeral'
    )
}}

WITH has_verified_address AS (
    SELECT a.user_id
    FROM
        {{ var ("compliance_public") }}.addresses AS a
    WHERE
        a.status = 'VERIFIED'
),

has_approved_secondary_document AS (
    SELECT
        k.owner_id,
        k.submitted_at AS latest_accepted_secondary_kyc_submmitted_at
    FROM
        {{ var ("compliance_public") }}.kyc_documents AS k
    WHERE
        k.status = 'ACCEPTED'
        AND k.doc_type != 'BVN'
)

SELECT DISTINCT
    owner_id AS user_id,
    true AS has_verified_address,
    true AS has_approved_secondary_document,
    latest_accepted_secondary_kyc_submmitted_at
FROM
    has_approved_secondary_document
INNER JOIN has_verified_address
    ON has_verified_address.user_id = has_approved_secondary_document.owner_id
