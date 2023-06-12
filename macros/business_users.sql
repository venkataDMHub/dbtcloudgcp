{% macro business_users() %}
    select distinct id
    from CHIPPER.{{var("core_public")}}.USERS
    where is_business = TRUE
{% endmacro %}

