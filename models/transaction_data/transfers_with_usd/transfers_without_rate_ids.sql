{{  config(
        materialized='incremental',
        unique_key='id',
        on_schema_change='append_new_columns', 
        schema='intermediate') }}

{%
set hardcoded_exchange_rates = { 
    'GHS': 0.19,
    'NGN': 0.0027,
    'KES': 0.0099,
    'UGX': 0.00027,
    'RWF': 0.0011,
    'TZS': 0.00043,
    'ZAR': 0.065 } %}


with ranked_exchage_rates as (
    select
        transfers.id,
        transfers.journal_id,
        transfers.origin_currency,
        transfers.origin_amount,
        transfers.exchange_rate,
        transfers.destination_currency,
        transfers.destination_amount,
        transfers.status,
        transfers.created_at,
        transfers.updated_at,
        transfers.exchange_rate_fee_percentage,
        transfers.origin_rate_id,
        transfers.destination_rate_id,
        transfers.flat_fee_amount,
        transfers.flat_fee_currency,
        transfers.base_modification_percentage,
        origin_er.rate as origin_rate,
        origin_er.timestamp as origin_rate_timestamp,
        destination_er.rate as destination_rate,
        destination_er.timestamp as destination_rate_timestamp,
        datediff(
            'second',
            origin_rate_timestamp,
            transfers.created_at
        ) as time_diff_in_seconds_to_nearest_available_rate,
        row_number() over(
            partition by transfers.id
            order by abs(time_diff_in_seconds_to_nearest_available_rate)
        ) as ranked_exchange_rate
    from chipper.{{ var("core_public") }}.transfers as transfers

    left join
        chipper.{{ var("core_public") }}.exchange_rates as origin_er on
            origin_er.currency = origin_currency
            and date_trunc('hour', created_at) = date_trunc('hour', origin_er.timestamp)
    left join
        chipper.{{ var("core_public") }}.exchange_rates as destination_er on
            destination_er.currency = destination_currency
            and date_trunc('hour', created_at) = date_trunc('hour', destination_er.timestamp)
    where origin_rate_id is null
        and destination_rate_id is null
        {% if is_incremental() %}
        -- this filter will only be applied on an incremental run
        and updated_at >= (select max(updated_at) from {{ this }})
    {% endif %}
    order by transfers.id
)

select
    ranked_exchage_rates.id,
    ranked_exchage_rates.journal_id,
    ranked_exchage_rates.origin_currency,
    ranked_exchage_rates.origin_amount,
    ranked_exchage_rates.exchange_rate,
    ranked_exchage_rates.destination_currency,
    ranked_exchage_rates.destination_amount,
    ranked_exchage_rates.status,
    ranked_exchage_rates.created_at,
    ranked_exchage_rates.updated_at,
    ranked_exchage_rates.exchange_rate_fee_percentage,
    ranked_exchage_rates.origin_rate_id,
    ranked_exchage_rates.destination_rate_id,
    ranked_exchage_rates.flat_fee_amount,
    ranked_exchage_rates.flat_fee_currency,
    ranked_exchage_rates.base_modification_percentage,
    case
        {% for currency,
        rate in hardcoded_exchange_rates.items() %}
        when origin_rate is null
            and origin_currency = '{{currency}}' then {{ rate }}
        {% endfor %}
        else origin_rate
    end as origin_rate,
    case
        {% for currency,
        rate in hardcoded_exchange_rates.items() %}
        when destination_rate is null
            and destination_currency = '{{currency}}' then {{ rate }}
        {% endfor %}
        else destination_rate
    end as destination_rate
from ranked_exchage_rates
where ranked_exchange_rate = 1
