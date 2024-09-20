{% macro athena__refresh_external_table(source_node) %}
    {# https://docs.aws.amazon.com/athena/latest/ug/partitions.html #}

    {%- set starting = [{ 'partition_by': [],'path': '' }] -%}
    {%- set parts_list = [] -%}
    {%- set finals = [] -%}

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
            {%- for partition in partitions -%}
                {%- if not loop.first -%}
                  {%- set starting = parts_list -%}
                  {%- set parts_list = [] -%}
                {%- endif -%}

                {%- for preexisting in starting -%}
                    {%- if partition.vals.macro -%}
                        {%- set vals = dbt_external_tables.render_from_context(partition.vals.macro, **partition.vals.args) -%}
                    {%- elif partition.vals is string -%}
                        {%- set vals = [partition.vals] -%}
                    {%- else -%}
                        {%- set vals = partition.vals -%}
                    {%- endif -%}
                    {%- for val in vals -%}
                        {%- set partition_parts = [] -%}

                        {%- for sub_part in preexisting.partition_by -%}
                          {%- do partition_parts.append(sub_part) -%}
                        {%- endfor -%}

                        {%- do partition_parts.append({'name': partition.name, 'value': val}) -%}
                        {%- set path_parts = preexisting.path ~ dbt_external_tables.render_from_context(partition.path_macro, partition.name, val) -%}
                        {%- set construct = {
                            'partition_by': partition_parts,
                            'path': path_parts
                        } -%}
                        {%- do parts_list.append(construct) -%}
                    {%- endfor -%}

                {%- endfor -%}

                {%- if loop.last -%}
                  {%- for part_spec in parts_list -%}
                    {%- do finals.append(part_spec) -%}
                  {%- endfor -%}
                {%- endif -%}

            {%- endfor -%}
            {%- set ddl = dbt_external_tables.redshift_alter_table_add_partitions(source_node, finals) -%}
            {{ return(ddl) }}

        {% endif %}
    {% endif %}
    {% do return([]) %}
{% endmacro %}