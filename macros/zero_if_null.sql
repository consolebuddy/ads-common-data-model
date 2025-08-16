{% macro zero_if_null(expr) %}
    coalesce({{ expr }}, 0)
{% endmacro %}
