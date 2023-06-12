{% macro generate_activation_sql(primary_currency, transfer_type) %}

{# Dictionary that maps country to days_joined_since to qualify for activation #}
    {% set country_mapping= {'NGN' : -7, 
                            'KES' : -30,
                            'UGX' : -7, 
                            'ZAR' : -30,
                            'GHS' : -30,
                            'RWF' : -30,
                            'TZS' : -30,
                            'USD' : -30,
                            'GBP' : -30,
                            'ZMW' : -30}
    %}

    {% set query %}

    with country_segment as (
    SELECT  expanded_users.user_id 
    FROM       {{ ref('expanded_users')}} as expanded_users
    LEFT JOIN  "CHIPPER".{{var("core_public")}}."USER_SEGMENTS" as base_users
        ON   expanded_users.user_id = base_users.user_id
    WHERE  primary_currency = '{{ primary_currency }}'
    AND  segment = 'BASE_AUDIENCE_FOR_ACTIVATION_CAMPAIGNS'
    AND  cast(created_at as date) between dateadd('day', {{country_mapping[primary_currency]}}, current_date()) and current_date()            
    ), 

    product_line_not_activated as (
    SELECT country_segment.user_id, expanded_ledgers.transfer_type
    FROM  country_segment 
    LEFT JOIN  {{ ref('expanded_ledgers')}} as expanded_ledgers
    ON   expanded_ledgers.main_party_user_id = country_segment.user_id
    AND  expanded_ledgers.transfer_type = '{{transfer_type}}'
    AND  is_original_transfer_reversed = FALSE
    )

    SELECT  distinct user_id 
    FROM  product_line_not_activated
    WHERE transfer_type is null

    {%- endset %}
    
{{ return(query) }}

{% endmacro %}
