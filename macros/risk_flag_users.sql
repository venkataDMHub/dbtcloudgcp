{% macro risk_flag_users() %}
    select 
    distinct user_id
    from {{ ref('risk_flags') }}
{% endmacro %}
