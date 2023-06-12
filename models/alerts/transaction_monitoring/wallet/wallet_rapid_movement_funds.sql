{% set currency_limits = {
    "ZAR": 4499,
} %}
select
    main_party_user_id as user_id,
    cast(ledger_timestamp as date) as triggered_at,
    array_agg(distinct transfer_id) as list_of_txns,
    ledger_currency,
    sum(case when ledger_amount > 0 then ledger_amount else 0 end) as sum_credits,
    sum(
        case when ledger_amount_in_usd > 0 then ledger_amount_in_usd else 0 end
    ) as sum_credits_usd,
    sum(case when ledger_amount < 0 then ledger_amount else 0 end) as sum_debits,
    sum(
        case when ledger_amount_in_usd < 0 then ledger_amount_in_usd else 0 end
    ) as sum_debits_usd
from {{ ref("expanded_ledgers") }}
where hlo_status in ('SETTLED', 'COMPLETED') and is_original_transfer_reversed = false

group by user_id, cast(ledger_timestamp as date), ledger_currency
having
    {% for currency, amount_limit in currency_limits.items() %}
    (
        sum_credits >= {{ amount_limit }}
        and ledger_currency = '{{ currency }}'
        and abs(sum_debits) >= 0.9 * sum_credits
    )
    {{ "or" if not loop.last }}
    {% endfor %}
