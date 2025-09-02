{% macro create_boolean_from_extract(column_name, pattern_dict) %}
    {%- for key, values in pattern_dict.items() %}
        CASE 
            WHEN
                {%- for string in values %}
                    {{ column_name }} LIKE '%{{ string }}%'
                    {%- if not loop.last %} OR {% endif -%}
                {%- endfor %}
            THEN 1 
            ELSE 0 
        END AS "{{ key }}"
        {%- if not loop.last %}, {% endif -%}
    {%- endfor %}
{% endmacro %}
