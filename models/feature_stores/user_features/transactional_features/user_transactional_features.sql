{% set product_bucket = ('purchases', 'p2p', 'investments', 'deposits') %}

{% set time_horizon_in_days = (7, 14, 21, 28, 90, 180) %}

SELECT
    e.user_id,
    {% for product in product_bucket %}
            {% for day_horizon in time_horizon_in_days %}
                ifnull(o.{{ product }}_count_first_{{ day_horizon }}_days, 0) 
                    AS {{ product }}_count_first_{{ day_horizon }}_days,
                ifnull(o.{{ product }}_value_in_usd_first_{{ day_horizon }}_days, 0)
                    AS {{ product }}_value_in_usd_first_{{ day_horizon }}_days,
                ifnull(r.{{ product }}_count_past_{{ day_horizon }}_days, 0)
                    AS {{ product }}_count_past_{{ day_horizon }}_days,
                ifnull(r.{{ product }}_value_in_usd_past_{{ day_horizon }}_days, 0) 
                    AS {{ product }}_value_in_usd_past_{{ day_horizon }}_days
                {{ "," if not loop.last }}
            {% endfor %}
        {{ "," if not loop.last }}
    {% endfor %}
FROM {{ ref("expanded_users") }} e
LEFT JOIN {{ ref("recent_transactions") }} r ON e.user_id = r.user_id
LEFT JOIN {{ ref("onboard_transactions") }} o ON e.user_id = o.user_id
