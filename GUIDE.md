# lakeflow-dbt guide

dbt's developer experience, Lakeflow Declarative Pipelines' execution model.
You write `models/*.sql` (or `*.py`) with `{{ ref() }}` / `{{ source() }}` /
`{{ config() }}`, schema.yml tests, seeds and selectors — `lfd` compiles the
project into declarative pipeline specs (`spark-pipeline.yml` + definition
files), not ordered DDL/DML.

## Install

```bash
pip install -e .
lfd --help            # same binary as `lakeflow-dbt`
```

## Concepts that differ from dbt

| dbt | lakeflow-dbt |
|---|---|
| `materialized='view'` | `CREATE VIEW` (auto→ `TEMPORARY VIEW` when it feeds a streaming table) |
| `materialized='temp_view'` | `CREATE TEMPORARY VIEW` |
| `materialized='table'` | `CREATE OR REFRESH MATERIALIZED VIEW` |
| `materialized='incremental'` | `STREAMING TABLE` + `CREATE FLOW ... INSERT BY NAME` |
| `materialized='ephemeral'` | inlined `WITH` CTE |
| SCD2 snapshots | `materialized='incremental', flow='auto_cdc'` (a flow config, not a materialization) |

## Quick start

```bash
cd examples/jaffle_lake
lfd parse                     # validate project
lfd compile                   # -> target/definitions/*.sql + spark-pipeline.yml + manifest.json
lfd ls --select marts         # list models like `dbt ls`
lfd test                      # -> target/tests/*.sql (set-level check queries)
lfd docs generate             # -> target/docs/index.html (lineage + columns)
lfd deploy                    # -> databricks.yml (Databricks Asset Bundle)
```

Deploy/run on Databricks:

```bash
lfd deploy && databricks bundle deploy
databricks bundle run jaffle_lake
# or locally, if `spark-pipelines` is installed:
lfd run                        # spark-pipelines run
lfd run --refresh 'order_events+'   # selective refresh (dbt selector syntax)
lfd run --full-refresh customers_scd2
```

## Config

`{{ config(...) }}` in the model header, or `config:` in schema.yml, or
`+key:` under `models:`/`seeds:` in `lakeflow_project.yml` (dir defaults).

| key | meaning |
|---|---|
| `materialized` | `view` `temp_view` `table` `incremental` `ephemeral` (aliases: `auto_cdc`, `snapshot` → `incremental`+`flow='auto_cdc'`) |
| `flow` | `append` (default) or `auto_cdc` — only on `incremental` |
| `snapshot` | `true` → `AUTO CDC ... FROM SNAPSHOT (q) WITH VERSION (v)` |
| `into` | extra flow into another streaming table — a model name or qualified `catalog.schema.table` (external sink) |
| `keys` `sequence_by` `stored_as` `track_history` `columns` `apply_as_deletes` `apply_as_truncates` `ignore_null_updates` `version` `once` | AUTO CDC options |
| `partition_cols` `cluster_by` `comment` `private` `tags` | dataset attributes |

AUTO CDC example:

```sql
{{ config(materialized='incremental', flow='auto_cdc',
          keys=['customer_id'], sequence_by='updated_at',
          stored_as='scd_type_2', ignore_null_updates=true,
          apply_as_deletes="op = 'DELETE'") }}
select * from {{ source('raw', 'customers_feed') }}
```

Multiple flows into one target:

```sql
-- order_events_eu.sql — a second append flow into order_events
{{ config(materialized='incremental', into='order_events') }}
select * from stream({{ source('raw', 'orders_eu') }})

-- external sink: qualified name, no model CREATE emitted
{{ config(materialized='incremental', into='main.archive.events') }}
select * from {{ ref('order_events') }}
```

## Python models

`models/**/*.py` define `def model(dbt, spark)` returning a DataFrame.
`dbt.config(...)`, `dbt.ref('name')`, `dbt.source('s','t')` work like Jinja:

```python
def model(dbt, spark):
    dbt.config(materialized='table')
    return (spark.read.table(dbt.ref('order_events'))
            .groupBy('customer_id').count())
```

Emits `pyspark.pipelines` (`dp`) definitions — `@dp.materialized_view`,
`create_streaming_table` + `@dp.append_flow`, `create_auto_cdc_flow`, and
`@dp.expect*` decorators for tests. Python models can't be `ephemeral`.

## Multiple pipelines per project

`pipelines:` maps a name to a selector (or a map with `select` plus
`catalog`/`schema`/`storage`/`configuration` overrides):

```yaml
pipelines:
  bronze: "ingestion resource_type:seed"
  gold:
    select: marts
    schema: gold_schema
    configuration: {pipelines.channel: PREVIEW}
```

Each pipeline compiles to `target/pipelines/<name>/` with its own
`spark-pipeline.yml` — one spec = one pipeline in the Lakeflow UI. Rules:

- each dataset is owned by exactly one pipeline (overlap → error)
- cross-pipeline `ref()` reads the table directly; `STREAM()` only within a pipeline
- `lfd deploy` emits a `databricks.yml` with one `resources.pipelines` entry per spec

## Selectors

Whitespace = union, commas = intersection, `+` for graph edges:

```
lfd ls --select 'stg_orders+'          # node + descendants
lfd ls --select 'tag:gold,marts'       # marts models tagged gold
lfd ls --select 'fqn:marts.customers'  # fully-qualified
lfd ls --select 'path:staging'         # directory subtree
lfd ls --select 'source:raw.orders'    # readers of a source
lfd ls --select 'config:flow:auto_cdc' # config key[:value]
lfd ls --select 'state:modified'       # vs target/manifest.json (or --state dir)
lfd ls --select 'resource_type:seed'
```

`state:` needs a previous manifest — compile once, or pass `--state <dir>`
(e.g. a manifest artifact from prod) for slim-CI-style rebuilds.

## Tests

Row-level tests (`not_null`, `accepted_values`, `accepted_range`,
`expression_is_true`) compile to `CONSTRAINT ... EXPECT` clauses (or
`@dp.expect*` decorators on Python models); `+on_violation: warn|drop|fail`.

Set-level tests (`unique`, `relationships`) can't be row expectations —
`lfd test` emits them as violation queries to `target/tests/<model>.sql`;
run them post-deploy with `spark-sql -f` or a scheduled notebook.

## Docs

`lfd docs generate` writes `target/docs/index.html` — pipeline grouping,
lineage edges, per-node materialization/dataset/refs/sources, tags,
descriptions and column docs from schema.yml. `lfd docs serve --port 8080`
serves it locally.
