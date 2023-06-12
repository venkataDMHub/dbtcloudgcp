-- depends_on: {{ ref('assets') }} 

Select 
    distinct user_id,

{% for cur in get_currency_list() %}
    SUM(IFF(CURRENCY = '{{ cur }}', latest_wallet_balance, 0)) AS LATEST_{{cur}}_BALANCE
    {{ "," if not loop.last}}
{% endfor %}

from {{ref('user_wallet_balances_horizontal')}}
{{ dbt_utils.group_by(n=1) }}
