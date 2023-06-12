SELECT
    DISTINCT linked_accounts.id AS linked_account_id,
    linked_accounts.user_id,
    linked_accounts.type,
    CASE WHEN mobile_money.linked_account_id IS NOT NULL THEN concat
            (
                mobile_money.carrier, ' ',
                mobile_money.country, ' ',
                mobile_money.phone
            )
        WHEN bank_accounts.linked_account_id IS NOT NULL THEN concat
            (
                banks.name, ' ',
                banks.country, ' - ',
                bank_accounts.account_name, ' ',
                bank_accounts.account_number
            )
        WHEN payment_cards.linked_account_id IS NOT NULL THEN concat
            (
                payment_cards.card_network, ' ending ',
                payment_cards.last_four, ' issued by BIN ',
                payment_cards.bin, ' - ',
                payment_cards.issuing_bank
            )
        WHEN nubans.linked_account_id IS NOT NULL THEN concat
            (
                'Nuban provided by ',
                nubans.provider, ' - ',
                nubans.account_name, ' ',
                nubans.account_number, ' with ',
                nubans.bank_name
            )
            WHEN railsbank_user_details.linked_account_id IS NOT NULL THEN concat
                        (
                    'UK account number provided by Railsbank - ',
                railsbank_user_details.account_name, ' ',
                railsbank_user_details.uk_account_number
            )
        ELSE 'other_payment_method'
    END AS linked_account_info,
    linked_accounts.created_at AS linked_account_created_at,
    linked_accounts.is_linked,
    linked_accounts.is_verified,
    linked_accounts.is_external,
    linked_accounts.marked_as_malicious,
    CASE WHEN linked_accounts.is_linked = TRUE THEN 'YES'
        WHEN linked_accounts.is_linked = FALSE THEN 'NO'
    END AS linked,
    CASE WHEN linked_accounts.is_verified = TRUE THEN 'YES'
        WHEN linked_accounts.is_verified = FALSE THEN 'NO'
    END AS verified,
    CASE WHEN linked_accounts.is_external = TRUE THEN 'YES'
        WHEN linked_accounts.is_external = FALSE THEN 'NO'
    END AS external
FROM
    {{var("core_public")}}.linked_accounts
LEFT JOIN
    {{var("core_public")}}.mobile_money ON linked_accounts.id = mobile_money.linked_account_id
LEFT JOIN
    {{var("core_public")}}.bank_accounts ON linked_accounts.id = bank_accounts.linked_account_id
LEFT JOIN
    {{var("core_public")}}.banks ON bank_accounts.bank_id = banks.id
LEFT JOIN
    {{var("core_public")}}.payment_cards ON linked_accounts.id = payment_cards.linked_account_id
LEFT JOIN
    {{var("core_public")}}.nubans ON linked_accounts.id = nubans.linked_account_id
LEFT JOIN
    {{var("core_public")}}.railsbank_user_details ON linked_accounts.id = railsbank_user_details.linked_account_id
