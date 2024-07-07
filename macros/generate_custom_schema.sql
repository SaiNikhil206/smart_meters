{% macro generate_custome_schema(custom_schema,node) %}

    {% set default_schema = target.schema %}
    {% if custom_schema is none %}

        {{ default_schema }}
    {% else %}
        {{ custom_schema | trim }}
    {% endif %}

{% endmacro %}
