-- Override default set_query_tag() to include the model name
-- See https://docs.getdbt.com/reference/resource-configs/snowflake-configs#query-tags
{% macro set_query_tag() -%}
  {% if model.name %}
    {% set original_query_tag = get_current_query_tag() %}
    {% if original_query_tag %}
      {% set new_query_tag = original_query_tag ~ ": " ~ model.name %} 
    {% else %}
      {% set new_query_tag = model.name %} 
    {% endif %}
    {{ log("Setting query_tag to '" ~ new_query_tag ~ "'. Will reset to '" ~ original_query_tag ~ "' after materialization.") }}
    {% do run_query("alter session set query_tag = '{}'".format(new_query_tag)) %}
    {{ return(original_query_tag)}}
  {% endif %}
  {{ return(none)}}
{% endmacro %}
