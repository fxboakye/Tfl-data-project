{#
    This macro parses the date_value and dropoff_datetime fields to datetime values
#}

{% macro parse_datetime(date_value) %}

    case 
        when {{ date_value }} like  '%-%' then PARSE_datetime("%Y-%m-%d %H:%M", {{date_value}})
        when {{ date_value }} like  '%/%' then PARSE_datetime("%d/%m/%Y %H:%M", {{date_value}})
    end 

{% endmacro %}