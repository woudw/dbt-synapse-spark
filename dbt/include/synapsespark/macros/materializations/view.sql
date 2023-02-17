{% materialization view, adapter='synapsespark' -%}
    {{ return(create_or_replace_view()) }}
{%- endmaterialization %}
