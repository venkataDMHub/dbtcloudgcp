SELECT
    data.created_at as created_at,
    data.status as status,
    data.currency as currency,
    data.error_message as error_message,
    data.carrier as carrier,
    data.data_provider as data_provider,
    data_options.description as description
FROM
    "CHIPPER".{{var("core_public")}}."DATA_PURCHASES" as data
    JOIN "CHIPPER".{{var("core_public")}}."DATA_OPTIONS" as data_options on data.data_option_id = data_options.id
