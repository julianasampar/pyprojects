{% macro standardize_nulls(column_name, additional_null_values=[], case_sensitive=false) %}
    {%- set default_null_values = [
        '',
        ' ',
        'null', 
        'unknown',
        'n/a',
        'none',
        'na',
        'nil',
        'empty',
        'blank',
        'missing',
        'undefined',
        'not available',
        'not applicable',
        'no data',
        'no value',
        '#n/a',
        '#null',
        'void',
        'absent'
    ] -%}
    
    {%- set all_null_values = default_null_values + additional_null_values -%}
    
    {%- if case_sensitive -%}
        {%- set base_column = 'TRIM(' ~ column_name ~ ')' -%}
    {%- else -%}
        {%- set base_column = 'LOWER(TRIM(' ~ column_name ~ '))' -%}
    {%- endif -%}

    CASE
        WHEN {{ base_column }} IN (
            {%- for v in all_null_values -%}
                '{{ v | lower if not case_sensitive else v }}'
                {%- if not loop.last -%}, {% endif -%}
            {%- endfor -%}
        )
        THEN NULL
        ELSE {{ base_column }}
    END
    
{% endmacro %}