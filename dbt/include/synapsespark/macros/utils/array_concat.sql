{% macro synapsespark__array_concat(array_1, array_2) -%}
    {% if env_var('DBT_SPARK_VERSION') == "2" %}
        {{ exceptions.raise_compiler_error("Array functions need Spark 3") }}
    {% else %}
        concat({{ array_1 }}, {{ array_2 }})
    {% endif %}
{%- endmacro %}
