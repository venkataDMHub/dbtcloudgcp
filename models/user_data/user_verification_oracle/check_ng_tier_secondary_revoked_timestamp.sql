{{
    config(
        materialized='ephemeral'
    )
}}

SELECT
    distinct owner_id as user_id,
    max(updated_at) as latest_secondary_kyc_revoked_at
FROM
    {{ var("compliance_public") }}.kyc_documents
WHERE
    status = 'REVOKED'
    AND doc_type != 'BVN'  {# BVN is primary document so we don't check for it #}
    AND issuing_country ='NG' 
{{ dbt_utils.group_by(n=1) }}
