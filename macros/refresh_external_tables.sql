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
            {%- for partition in partitions -%}
                {%- if loop.first -%}
                  {%- set partition_list = [{ 'partition_by': [],'path': '' }] -%}
                {% else %}
                  {%- set partition_list = partition_specs -%}
                {%- endif -%}

                {%- set partition_specs = [] -%}
                {%- for preexisting in partition_list -%}
                    {%- if partition.vals.macro -%}
                        {%- set vals = dbt_external_tables.render_from_context(partition.vals.macro, **partition.vals.args) -%}
                    {%- elif partition.vals is string -%}
                        {%- set vals = [partition.vals] -%}
                    {%- else -%}
                        {%- set vals = partition.vals -%}
                    {%- endif -%}


                    {# Allow the use of custom 'key' in path_macro (path.sql) #}
                    {# By default, take value from source node 'external.partitions.name' from raw yml #}
                    {# Useful if the data in s3 is saved with a prefix/suffix path 'path_macro_arg' other than 'external.partitions.name' #}
                    {%- if partition.path_macro_arg -%}
                        {%- set path_macro_arg = partition.path_macro_arg -%}
                    {%- else -%}
                        {%- set path_macro_arg = partition.name -%}
                    {%- endif -%}


                    {%- for val in vals -%}
                        {# For each preexisting item, add a new one #}
                        {%- set partition_parts = [] -%}

                        {%- for sub_part in preexisting.partition_by -%}
                          {%- do partition_parts.append(sub_part) -%}
                        {%- endfor -%}

                        {%- do partition_parts.append({'name': partition.name, 'value': val}) -%}

                        {# Concatenate path #}
                        {%- set path_parts = preexisting.path ~ dbt_external_tables.render_from_context(partition.path_macro, path_macro_arg, val) -%}

                        {%- set construct = {
                            'partition_by': partition_parts,
                            'path': path_parts
                        } -%}
                        {%- do partition_specs.append(construct) -%}
                    {%- endfor -%}

                {%- endfor -%}

                {%- if loop.last -%}
                  {%- for part_spec in partition_specs -%}
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