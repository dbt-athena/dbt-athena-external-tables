**PROOF OF CONCEPT - USE AT OWN RISK**

Experimental decoupled `dbt-external-tables` for consideration by dbt-athena-community maintainers. See https://github.com/dbt-athena/dbt-athena/issues/633

Can we implement dbt-external-tables without having to merge into dbt-external-tables?

# Usage

A fully working example project provided. See [example_project](example_project/README.md)

## Add dbt-external-tables

Package provides the target-independent implementation

```yaml
  - package: dbt-labs/dbt_external_tables
    version: ['>=0.8.7']
```

## Add This Package

Package provides athena-specific implementation

```yaml
  - git: https://github.com/brabster/dbt-athena-external-tables.git
    revision: main
```

## Override Macro Search Order in Project

```yaml
dispatch:
  - macro_namespace: dbt_external_tables
    search_order: [dbt_athena_external_tables, dbt_external_tables]
```

## Follow Instructions for dbt-external-tables
