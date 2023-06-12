-- For example,
-- 551c6380-6a94-11eb-bbab-3fa4e14273fa, 2021-02-09T05:10:36, Odumosu, Oluwatunise, 1999-11-07
-- are the user_id, user_creation time, verified first name, verified last name, date of birt
-- more column information available in documentation
WITH contacts AS (
    SELECT
        c.user_id,
        c.identifier,
        c.type,
        ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY created_at DESC) AS create_rank
    FROM
        chipper.{{var("core_public")}}.contacts AS c
),

contacts_latest AS (
    SELECT
        *
    FROM
        contacts
    WHERE
        create_rank = 1
),

kyc AS (
    SELECT
        kyc.id AS kyc_id,
        kyc.owner_id AS owner_id,
        kyc.status,
        kyc.doc_type,
        kyc.ownership_proof_url,
        kyc.submitted_at,
        ROW_NUMBER() OVER(PARTITION BY kyc.owner_id ORDER BY submitted_at DESC) AS submitted_rank
    FROM
        chipper.{{var("compliance_public")}}.kyc_documents AS kyc
),

kyc_latest AS (
    SELECT
        *
    FROM
        kyc
    WHERE
        submitted_rank = 1
)

SELECT
    ui.user_id AS user_id,
    u.created_at AS user_account_created_at,
    ui.first_name AS verified_first_name,
    ui.last_name AS verified_last_name,
    ui.dob AS dob,
    ui.gender AS gender,
    ui.nationality AS nationality,
    ui.city_of_birth AS city_of_birth,
    ui.country_of_birth AS country_of_birth,
    ut.tier AS tier,
    u.first_name AS first_name,
    u.last_name AS last_name,
    u.primary_currency AS primary_currency,
    u.tag AS tag,
    c.identifier AS identifier,
    c.type AS identifier_type,
    kyc_latest.kyc_id AS kyc_id,
    kyc_latest.status AS status,
    kyc_latest.doc_type AS doc_type,
    kyc_latest.ownership_proof_url AS ownership_proof_url,
    kyc_latest.submitted_at AS kyc_submitted_at,
    TIMEDIFF(years, ui.dob, CURRENT_DATE()) AS user_age
FROM
    chipper.{{var("core_public")}}.users AS u
LEFT JOIN chipper.{{var("compliance_public")}}.user_info AS ui
    ON ui.user_id = u.id
LEFT JOIN chipper.{{var("compliance_public")}}.user_tiers AS ut
    ON ut.user_id = u.id
LEFT JOIN kyc_latest
    ON u.id = kyc_latest.owner_id
LEFT JOIN contacts_latest AS c
    ON c.user_id = u.id
    