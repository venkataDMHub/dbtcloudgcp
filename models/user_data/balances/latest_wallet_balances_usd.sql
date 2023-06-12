-- depends_on: {{ ref('assets') }} 

Select 
    distinct user_id,

{% for cur in get_currency_list() %}
    SUM(IFF(CURRENCY = '{{ cur }}', latest_wallet_balance_in_usd, 0)) AS LATEST_{{cur}}_BALANCE_USD,
{% endfor %}

{% for cur in get_currency_list() %}
	{% if cur in get_currency_list() %}
     	LATEST_{{cur}}_BALANCE_USD 
         {{ "+" }} 
	{%  endif %}
    {{ 0 if loop.last}}  	 
{% endfor %} as TOTAL_BALANCE_USD ,

{% for cur in get_currency_list() %}
	{% if cur in get_fiat_currency_list() %}
     	LATEST_{{cur}}_BALANCE_USD 
         {{ "+" }} 
	{%  endif %}
    {{ 0 if loop.last}}  	 
{% endfor %} as TOTAL_FIAT_BALANCE_USD, 

{% for cur in get_currency_list() %}
	{% if cur in get_crypto_currency_list() %}
     	LATEST_{{cur}}_BALANCE_USD 
         {{ "+" }} 
	{%  endif %}
    {{ 0 if loop.last}}  	 
{% endfor %} as TOTAL_CRYPTO_BALANCE_USD 

from {{ref('user_wallet_balances_horizontal')}}
{{ dbt_utils.group_by(n=1) }}
