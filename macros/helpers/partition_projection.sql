{% macro partition_projection_properties(source_node) %}
    {# https://docs.aws.amazon.com/athena/latest/ug/partition-projection.html #}
    {%- set external = source_node.external -%}
    {%- set properties = ["'projection.enabled'='true'"] -%}
    {%- for partition in external.partitions if partition.get('projection') -%}
        {%- set column = partition.name | replace('`', '') | trim -%}
        {%- for key, value in partition.projection.items() -%}
            {%- if value is iterable and value is not string and value is not mapping -%}
                {%- set rendered = value | join(',') -%}
            {%- else -%}
                {%- set rendered = value -%}
            {%- endif -%}
            {%- do properties.append("'projection." ~ column ~ "." ~ key ~ "'='" ~ rendered ~ "'") -%}
        {%- endfor -%}
    {%- endfor -%}
    {%- if external.get('storage_location_template') -%}
        {%- do properties.append("'storage.location.template'='" ~ external.storage_location_template ~ "'") -%}
    {%- endif -%}
    {{ return(properties) }}
{% endmacro %}
