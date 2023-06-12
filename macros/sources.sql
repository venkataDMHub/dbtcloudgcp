{% macro source(source_name, table_name) %}
    {% set rel = builtins.source(source_name, table_name) %}
    {% if source_name.startswith('fivetran_') %}
        {% set subquery %}
            (select * from {{ rel }} where _fivetran_deleted is null or _fivetran_deleted = False)
        {% endset %}       
        {% do return(subquery) %} 
    {% else %}
        {% do return(rel) %}
    {% endif %}
{% endmacro %}
