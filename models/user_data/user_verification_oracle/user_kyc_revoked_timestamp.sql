{{
    config(
        materialized='ephemeral'
    )
}}

SELECT
    distinct owner_id as user_id,
    max(updated_at) as user_kyc_revoked_timestamp
FROM
    {{ var("compliance_public") }}.kyc_documents
WHERE
    status = 'REVOKED'
{{ dbt_utils.group_by(n=1) }}
