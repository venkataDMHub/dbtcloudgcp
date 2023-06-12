{% set primary_currency = ("USD", "GHS", "ZAR", "NGN", "UGX") %}

{% set total_amount_limits = {
    "USD": 1,
    "GHS": 15,
    "ZAR": 18,
    "NGN": 435,
    "UGX": 3738,
} %}

{% set days_limit = 120 %}


with
    main as (
        select
            *,
            row_number() over (
                partition by main_party_user_id order by hlo_created_at desc
            ) as row_num,
            timestampdiff(
                day,
                hlo_created_at,
                lag(hlo_created_at) over (
                    partition by main_party_user_id order by hlo_created_at desc
                )
            ) as diff
        from {{ ref("expanded_ledgers") }} as expanded_ledgers
        left join
            {{ ref("expanded_users") }} as expanded_users
            on expanded_ledgers.main_party_user_id = expanded_users.user_id
        where
            hlo_status in ('SETTLED', 'COMPLETED')
            and journal_type in ('ISSUED_CARD')
            and primary_currency in {{primary_currency}}
        order by main_party_user_id, hlo_created_at desc
    )
select
    main_party_user_id as user_id,
    ledger_amount_in_usd as amount,
    transfer_type,
    array_construct(transfer_id) as list_of_txns,
    hlo_created_at as triggered_at,
    diff as diff_days,
    ledger_currency,
    ledger_amount
from main
where
    diff >= {{days_limit}} and 
    {% for currency, amount_limit in total_amount_limits.items() %}
    (ledger_currency = '{{currency}}' and ledger_amount_in_usd = {{ amount_limit }})
    {{ "or" if not loop.last }}
    {% endfor %}
    and transfer_type like '%FUNDING%'
