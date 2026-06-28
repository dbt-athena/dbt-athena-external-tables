**PROOF OF CONCEPT - USE AT OWN RISK**

Experimental decoupled `dbt-external-tables`, now maintained by dbt-athena-community maintainers. See https://github.com/dbt-athena/dbt-athena/issues/633, update any existing references to new `dbt-athena` owner.

# Usage

A fully working example project provided. See [example_project](example_project)

## Add dbt-external-tables

Package provides the target-independent implementation

```yaml
  - package: dbt-labs/dbt_external_tables
    version: ['>=0.8.7']
```

## Add This Package

Package provides athena-specific implementation

```yaml
  - git: https://github.com/dbt-athena/dbt-athena-external-tables.git
    revision: main
```

## Override Macro Search Order in Project

```yaml
dispatch:
  - macro_namespace: dbt_external_tables
    search_order: [dbt_athena_external_tables, dbt_external_tables]
```

## Follow Instructions for dbt-external-tables

## Partition Projection

[Partition projection](https://docs.aws.amazon.com/athena/latest/ug/partition-projection.html)
lets Athena compute partition values at query time instead of reading them from
the Glue catalog, so no `MSCK REPAIR TABLE` or `ALTER TABLE ADD PARTITION` is
needed to register partitions.

Set `partition_projection: true` on the source. The refresh step (`MSCK` /
`ALTER`) is then skipped, and the projection table properties are generated from
a `projection` config on each partition:

```yaml
sources:
  - name: my_source
    tables:
      - name: events
        external:
          location: 's3://my-bucket/events/'
          file_format: parquet
          partition_projection: true
          partitions:
            - name: fiscal_year
              data_type: int
              projection:
                type: integer
                range: "2009,2050"
```

Each key under `projection` is emitted as `projection.<column>.<key>`, so any
projection type (`integer`, `enum`, `date`, `injected`) is supported. List
values are joined with commas (`range: [2009, 2050]` → `2009,2050`).

`projection.enabled` is always added. When the data does not follow the default
Hive layout (`<column>=<value>`), provide `storage_location_template`:

```yaml
        external:
          partition_projection: true
          storage_location_template: 's3://my-bucket/events/${timestamp}'
          partitions:
            - name: "`timestamp`"
              data_type: string
              projection:
                type: date
                format: "yyyy/MM/dd"
                range: "2020/01/01,NOW"
                interval: 1
                interval.unit: DAYS
```

Any `table_properties` you also set are merged with the generated projection
properties.
