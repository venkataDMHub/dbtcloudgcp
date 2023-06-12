{% set transfer_type = 'DEPOSITS_SETTLED' %}

{% set currency_list = ('GBP','USD') %}

{% set payment_card_type = ('DEBIT','Debit') %}

WITH settled_deposits AS (
    SELECT
        transfer_id,
        transfer_type,
        journal_type,
        hlo_status,
        ledger_currency,
        ledger_amount_in_usd,
        hlo_created_at,
        main_party_user_id
    FROM
        {{ ref('expanded_ledgers') }}
    WHERE
        transfer_type IN ('{{ transfer_type }}')
        AND ledger_currency IN {{ currency_list }}
)

SELECT
    deposits_using_cards.payment_card_card_type,
    settled_deposits.hlo_created_at AS triggered_at,
    settled_deposits.ledger_amount_in_usd AS amount_in_usd,
    settled_deposits.main_party_user_id AS user_id,
    settled_deposits.ledger_currency AS currency,
    to_array(settled_deposits.transfer_id) AS list_of_txns
FROM
    {{ ref('deposits_using_cards') }} AS deposits_using_cards
INNER JOIN settled_deposits ON deposits_using_cards.transfer_id = settled_deposits.transfer_id
WHERE
    {# as per LRC Deposits to the cards made by using other than Debit card is considered unauthorized funding sources#}
    payment_card_card_type NOT IN {{ payment_card_type }}
