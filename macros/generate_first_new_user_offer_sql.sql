{% macro generate_first_new_user_offer_sql(primary_currency) %}

{# Dictionary that maps country to days_joined_since to qualify for activation #}
    {% set country_mapping= {'NGN' : -7, 
                            'KES' : -7,
                            'UGX' : -7, 
                            'ZAR' : -7,
                            'GHS' : -7,
                            'RWF' : -7,
                            'TZS' : -7,
                            'USD' : -7,
                            'GBP' : -7,
                            'ZMW' : -7}
    %}

    {% set query %}

    with country_segment as (
    SELECT  expanded_users.user_id 
    FROM       {{ ref('expanded_users')}} as expanded_users
    LEFT JOIN  "CHIPPER".{{var("compliance_public")}}."USER_TIERS" as user_tiers
        ON   expanded_users.user_id = user_tiers.user_id
    WHERE  primary_currency = '{{ primary_currency }}'
 

    AND (cast(user_tiers.updated_at as date) BETWEEN dateadd('day', {{country_mapping[primary_currency]}}, current_date()) AND current_date() 
            AND user_tiers.tier in ({{ verified_tiers() }}))
    OR  (cast(expanded_users.created_at as date) BETWEEN dateadd('day', {{country_mapping[primary_currency]}}, current_date()) AND current_date()
             AND user_tiers.tier not in ({{ verified_tiers() }}))
    )


    SELECT  distinct user_id 
    FROM  country_segment

    {%- endset %}

{{ return(query) }}

{% endmacro %}
