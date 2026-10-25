# lakeflow-dbt (`lfd`)

A dbt-style developer experience that compiles to **Lakeflow Declarative
Pipelines** (a.k.a. Spark Declarative Pipelines, formerly Delta Live Tables).

You write models exactly like dbt — `.sql` files with `{{ ref() }}`,
`{{ source() }}`, `{{ config() }}`, `schema.yml` tests — but `lfd compile`
emits a *declarative* pipeline: datasets and flows that the engine schedules,
not imperative `CREATE/INSERT` statements run in order.

## Install

```bash
pip install -e .
lfd --help          # or: lakeflow-dbt
```

## Quickstart

```bash
lfd init my_lake            # scaffold a project
cd my_lake
lfd parse                   # validate models + graph
lfd compile                 # emit target/definitions/*.sql + spark-pipeline.yml
lfd run --dry-run           # spark-pipelines dry-run (needs pyspark[pipelines])
```

Try it on the bundled example: `cd examples/jaffle_lake && lfd compile`.

## A model looks like dbt

```sql
-- models/marts/order_events.sql
{{ config(materialized='incremental', on_violation='drop') }}

select o.order_id, t.order_total
from {{ ref('stg_orders') }} o
join {{ ref('order_item_totals') }} t on o.order_id = t.order_id
```

```yaml
# models/schema.yml
version: 2
sources:
  - name: raw
    schema: raw_data
    tables:
      - name: orders
        streaming: true          # read via STREAM() inside incremental models
models:
  - name: order_events
    columns:
      - name: order_id
        tests: [not_null]
```

## What each dbt concept compiles to

| dbt concept                    | Declarative pipeline output                                   |
|--------------------------------|---------------------------------------------------------------|
| `materialized='view'`          | `CREATE VIEW` — or `CREATE TEMPORARY VIEW` + `STREAM()` reads when it feeds a streaming table |
| `materialized='table'`         | `CREATE OR REFRESH MATERIALIZED VIEW`                         |
| `materialized='incremental'`   | `CREATE OR REFRESH STREAMING TABLE` + `CREATE FLOW ... INSERT INTO ... BY NAME` |
| `materialized='ephemeral'`     | inlined as a `WITH` CTE into each dependent                   |
| `materialized='temp_view'`     | `CREATE TEMPORARY VIEW`                                       |
| `{{ ref('m') }}`               | dataset name; `STREAM(name)` inside flows when upstream is streaming |
| `{{ source('s','t') }}`        | `catalog.schema.table` from `sources:`; `STREAM(...)` when flagged `streaming: true` and read inside a flow |
| `not_null` test                | `CONSTRAINT ... EXPECT (col IS NOT NULL)`                     |
| `accepted_values` test         | `CONSTRAINT ... EXPECT (col IN (...))`                        |
| `accepted_range`, `expression_is_true` | row-level `EXPECT (...)` predicates                  |
| `unique`, `relationships`      | skipped with a warning (set-level checks can't be row expectations) |
| `description`                  | `COMMENT '...'`                                               |
| `cluster_by`, `partition_cols`, `tblproperties` | `CLUSTER BY` / `PARTITIONED BY` / `TBLPROPERTIES` clauses |
| `on_violation: warn/drop/fail` | plain `EXPECT` / `ON VIOLATION DROP ROW` / `FAIL UPDATE`      |
| `--select m / +m / m+ / tag:t` | dbt-style selection; `compile` also pulls in ancestors so the emitted pipeline is self-contained |
| `{{ var('x') }}` / `--vars`    | project `vars:` + CLI overrides                                |
| `macros/`                      | Jinja `{% import %}`/`{% include %}` from `macro-paths`        |

## `lakeflow_project.yml` (dbt_project.yml analogue)

```yaml
name: jaffle_lake
catalog: main            # -> spec catalog
schema: jaffle_lake      # -> spec database
storage: /Volumes/...    # -> spec storage (streaming checkpoints)
model-paths: [models]
target-path: target
vars: {currency: USD}
configuration:           # -> spec Spark conf
  spark.sql.shuffle.partitions: "200"
models:
  jaffle_lake:
    staging: {+materialized: view}
    marts:   {+materialized: table, +on_violation: drop}
```

## Running on Databricks

`lfd run` shells out to the `spark-pipelines` CLI
(`pip install 'pyspark[pipelines]'`):

```bash
lfd run --select marts+ -- --conf spark.sql.shuffle.partitions=400
lfd run --full-refresh order_events --remote sc://cluster:15002
```

Or take `target/` (spec + `definitions/`) into your own bundle/job — point the
pipeline's libraries at `definitions/**`.

## Deliberately *not* dbt

- **No hooks, no `run_query`, no incremental merge boilerplate.** Everything is
  declared; the engine owns ordering, retries, and incremental checkpoints.
- **Tests are expectations, not queries.** Row-level checks ride inside the
  dataset definition and can `DROP ROW` or `FAIL UPDATE`; set-level checks like
  `unique` don't fit the declarative model and produce warnings.
- **`--select` semantics are for `ls`.** `compile`/`run` always close over
  ancestors — a pipeline spec must resolve every dataset name it references.

## Tests

```bash
python3 -m pytest tests -q
```
