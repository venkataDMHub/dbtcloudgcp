{% macro get_fiat_currency_list() %}

{% call statement('currencies', fetch_result=True) %}
    
    select distinct id
    from {{ref('assets')}}
	  where id != 'NONE' and type = 'FIAT_CURRENCY'

{% endcall %}

{% set currencies = load_result('currencies')['data'] %}

  {{ return(currencies|map(attribute=0)|list )}}
  
{% endmacro %}
