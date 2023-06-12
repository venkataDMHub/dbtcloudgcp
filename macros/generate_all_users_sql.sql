{% macro generate_all_users(primary_currency) %}

    {% set query %}

    
    SELECT distinct expanded_users.user_id as user_id
    FROM       {{ ref('expanded_users')}} as expanded_users
    LEFT JOIN  "CHIPPER".{{var("core_public")}}."USER_SEGMENTS" as base_users
        ON   expanded_users.user_id = base_users.user_id
    WHERE  primary_currency = '{{ primary_currency }}'
    AND  segment = 'BASE_AUDIENCE_FOR_ACTIVATION_CAMPAIGNS'

    {%- endset %}
    
{{ return(query) }}

{% endmacro %}
