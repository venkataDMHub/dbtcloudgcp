SELECT
    user_id,
    id AS contact_id,
    type,
    identifier,
    verified,
    created_at
FROM {{var("core_public")}}.contacts
