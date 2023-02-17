{% macro synapsespark__concat(fields) -%}
    concat({{ fields|join(', ') }})
{%- endmacro %}
