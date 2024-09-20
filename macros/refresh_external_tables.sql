{% macro athena__refresh_external_table(source_node) %}
  {# https://docs.aws.amazon.com/athena/latest/ug/partitions.html #}

  {%- set starting = [
    {
      'partition_by': [],
      'path': ''
    }
  ] -%}

  {%- set ending = [] -%}
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
        {%- for partition in partitions -%}
          {%- if not loop.first -%}
            {%- set starting = ending -%}
            {%- set ending = [] -%}
          {%- endif -%}
          {%- for preexisting in starting -%}
            {%- if partition.vals.macro -%}
              {%- set vals = render_from_context(partition.vals.macro, **partition.vals.args) -%}
            {%- elif partition.vals is string -%}
              {%- set vals = [partition.vals] -%}
            {%- else -%}
              {%- set vals = partition.vals -%}
            {%- endif -%}

            {# Allow the use of custom 'key' in path_macro (path.sql) #}
            {# By default, take value from source node 'external.partitions.name' from raw yml #}
            {# Useful if the data in s3 is saved with a prefix/suffix path 'path_macro_key' other than 'external.partitions.name' #}
            {%- if partition.path_macro_key -%}
              {%- set path_macro_key = partition.path_macro_key -%}
            {%- else -%}
              {%- set path_macro_key = partition.name -%}
            {%- endif -%}

            {%- for val in vals -%}
              {# For each preexisting item, add a new one #}
              {%- set next_partition_by = [] -%}
              {%- for prexist_part in preexisting.partition_by -%}
                {%- do next_partition_by.append(prexist_part) -%}
              {%- endfor -%}
              {%- do next_partition_by.append({'name': partition.name, 'value': val}) -%}
              {# Concatenate path #}
              {%- set concat_path = preexisting.path ~ render_from_context(partition.path_macro, path_macro_key, val) -%}
              {%- do ending.append({'partition_by': next_partition_by, 'path': concat_path}) -%}
            {%- endfor -%}
          {%- endfor -%}
          {%- if loop.last -%}
            {%- for end in ending -%}
              {%- do finals.append(end) -%}
            {%- endfor -%}
          {%- endif -%}
        {%- endfor -%}
        {%- set ddl = dbt_external_tables.redshift_alter_table_add_partitions(source_node, finals) -%}
        {{ return(ddl) }}
    {% endif %}
  {% else %}
    {% do return([]) %}
  {% endif %}
{% endmacro %}