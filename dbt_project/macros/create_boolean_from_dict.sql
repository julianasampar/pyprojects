{% macro create_boolean_from_dict(columns_dict, true_values=none, false_values=none, default_value=0) %}
    {%- if false_values is none -%}
        {%- set false_values = ['no', 'false', '0', 'nodamage'] -%}
    {%- endif -%}
    {%- if true_values is none -%}
        {%- set true_values = ['yes', 'true', '1', 'damage'] -%}
    {%- endif -%}

    {%- for key, value in columns_dict.items() -%}
    CASE 
        WHEN LOWER(TRIM({{ key }})) IN (
            {%- for value in true_values -%}
                '{{ value | lower }}'
                {%- if not loop.last -%},{%- endif -%}
            {%- endfor -%}
        ) THEN 1
        WHEN LOWER(TRIM({{ key }})) IN (
            {%- for value in false_values -%}
                '{{ value | lower }}'
                {%- if not loop.last -%},{%- endif -%}
            {%- endfor -%}
        ) THEN 0
        ELSE {{ default_value }}
        END AS {% if value is none %} {{ key }}
            {% else %} {{ value }}
            {% endif %}
        {%- if not loop.last -%},{%- endif -%}
    {%- endfor -%}
{% endmacro %}