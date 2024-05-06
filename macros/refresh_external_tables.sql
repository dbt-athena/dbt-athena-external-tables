{% macro athena__refresh_external_table(source_node) %}
    {# https://docs.aws.amazon.com/athena/latest/ug/partitions.html #}
    {%- set partitions = source_node.external.partitions -%}
    {%- set hive_compatible_partitions = source_node.external.get('hive_compatible_partitions', false) -%}
    {%- if partitions -%}
        {%- if hive_compatible_partitions -%}
            {% set ddl -%}
                msck repair table {{source(source_node.source_name, source_node.name).render_hive()}}
            {%- endset %}
            {{ return([ddl]) }}
        {% else %}
            {# https://docs.aws.amazon.com/athena/latest/ug/alter-table-add-partition.html #}
            {%- set part_len = partitions|length -%}
            {%- set finals = [] -%}
            {%- if execute -%}

                {%- for partition in partitions %} (
                    {%- set part_num = loop.index -%}
                    {%- if partition.vals.macro -%}
                        {%- set vals = dbt_external_tables.render_from_context(partition.vals.macro, **partition.vals.args) -%}
                    {%- elif partition.vals is string -%}
                        {%- set vals = [partition.vals] -%}
                    {%- else -%}
                        {%- set vals = partition.vals -%}
                    {%- endif -%}

                    {%- set partition_parts = [] -%}
                    {%- set path_parts = [] -%}

                    {%- for val in vals %}
                        {%- do partition_parts.append({
                            'name': '"{{ partition.name }}"',
                            'value': '"{{ val }}"'
                        }) -%}
                        {%- do path_parts.append('"{{ dbt_external_tables.render_from_context(partition.path_macro, partition.name, val) }}"') -%}

                    {%- endfor -%}
                {%- endfor -%}

                {%- set construct = {
                    'partition_by': partition_parts,
                    'path': path_parts | join('/')
                }  -%}
                {% do finals.append(construct) %}

            {%- endif -%}
            {%- set ddl = dbt_external_tables.redshift_alter_table_add_partitions(source_node, finals) -%}
            {{ return(ddl) }}
            {% do return([]) %}
        {% endif %}
    {% endif %}
    {% do return([]) %}
{% endmacro %}