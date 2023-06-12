{% macro internal_users() %}
    select 
    distinct id
    from CHIPPER.{{var("core_public")}}.USERS
    where id like any ('bot-%', 'base-%', 'issuer-%', 'chipper-%')
{% endmacro %}
