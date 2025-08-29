{% macro standardize_nulls(column_name, additional_null_values=[], case_sensitive=false) %}
    {%- set default_null_values = [
        '',
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

    {%- set result = base_column -%}
    
    {%- for null_value in all_null_values -%}
        {%- if not case_sensitive -%}
            {%- set processed_value = null_value | lower -%}
        {%- else -%}
            {%- set processed_value = null_value -%}
        {%- endif -%}
        
        {%- set result = 'NULLIF(' ~ result ~ ', \'' ~ processed_value ~ '\')' -%}
    {%- endfor -%}

    {{ result }}
{% endmacro %}