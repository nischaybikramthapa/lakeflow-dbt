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
lfd seed                    # compile only seed CSVs
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
| `materialized='auto_cdc'`      | `TEMPORARY VIEW` staging + `STREAMING TABLE` + `CREATE FLOW ... AUTO CDC INTO ... FROM STREAM` |
| `materialized='snapshot'`      | `STREAMING TABLE` + `CREATE FLOW ... AUTO CDC ... FROM SNAPSHOT (query) WITH VERSION (expr)` |
| seed CSVs under `seed-paths`   | `CREATE OR REFRESH MATERIALIZED VIEW` over a `VALUES` literal (types inferred) |
| `{{ ref('m') }}`               | dataset name; `STREAM(name)` inside flows when upstream is streaming |
| `{{ source('s','t') }}`        | `catalog.schema.table` from `sources:`; `STREAM(...)` when flagged `streaming: true` and read inside a flow |
| `not_null` test                | `CONSTRAINT ... EXPECT (col IS NOT NULL)`                     |
| `accepted_values` test         | `CONSTRAINT ... EXPECT (col IN (...))`                        |
| `accepted_range`, `expression_is_true` | row-level `EXPECT (...)` predicates                  |
| `unique`, `relationships`      | skipped with a warning (set-level checks can't be row expectations) |
| `description`                  | `COMMENT '...'`                                               |
| `cluster_by`, `partition_cols`, `tblproperties` | `CLUSTER BY` / `PARTITIONED BY` / `TBLPROPERTIES` clauses |
| `on_violation: warn/drop/fail` | plain `EXPECT` / `ON VIOLATION DROP ROW` / `FAIL UPDATE`      |
| `keys`, `sequence_by`, `stored_as`, `track_history`, `apply_as_deletes/truncates`, `ignore_null_updates`, `columns`, `once` | `AUTO CDC INTO` clauses on the flow |
| `version` (snapshot)           | `WITH VERSION (expr)` for ordering snapshots                   |
| `into: other_model`            | flow-only model: no dataset DDL, just an extra `FLOW ... INTO` another model's streaming table (multi-append) |
| `--select m / +m / m+ / tag:t / resource_type:seed` | dbt-style selection; `compile` also pulls in ancestors so the emitted pipeline is self-contained |
| `{{ var('x') }}` / `--vars`    | project `vars:` + CLI overrides                                |
| `macros/`                      | Jinja `{% import %}`/`{% include %}` from `macro-paths`        |

## `lakeflow_project.yml` (dbt_project.yml analogue)

```yaml
name: jaffle_lake
catalog: main            # -> spec catalog
schema: jaffle_lake      # -> spec database
storage: /Volumes/...    # -> spec storage (streaming checkpoints)
model-paths: [models]
seed-paths: [seeds]
target-path: target
vars: {currency: USD}
configuration:           # -> spec Spark conf
  spark.sql.shuffle.partitions: "200"
models:
  jaffle_lake:
    staging: {+materialized: view}
    marts:   {+materialized: table, +on_violation: drop}
seeds:                   # optional +config tree, same shape as models:
  jaffle_lake: {+materialized: table}
```

## AUTO CDC / snapshots (dbt `snapshot`, done declaratively)

```sql
{{ config(materialized='auto_cdc', keys=['customer_id'],
          sequence_by='updated_at', stored_as='scd_type_2',
          track_history={'except': ['updated_at']},
          apply_as_deletes="op = 'DELETE'") }}
select * from {{ source('raw', 'customers_feed') }}
```

`auto_cdc` expects a change feed (`op`/`updated_at` style rows) and emits
`AUTO CDC INTO ... FROM STREAM(<staging temp view>)`. `snapshot` expects
periodic full snapshots and emits `AUTO CDC ... FROM SNAPSHOT (query)`
(`version` orders the snapshots; `sequence_by` is rejected).

### Multiple flows into one table

SDP lets several flows append to the same streaming table. Set `into:` to
make a model flow-only — it emits no `CREATE` of its own, just an extra
`FLOW ... INTO` another model's table:

```sql
{{ config(materialized='incremental', into='order_events') }}
select order_id, ... from {{ source('raw', 'orders_eu') }}
```

`ref()` to a flow-only model resolves to its target table, and refresh
selectors resolve to the target (the flow-only model has no dataset).

## Running on Databricks

`lfd run` shells out to the `spark-pipelines` CLI
(`pip install 'pyspark[pipelines]'`):

```bash
lfd run --select marts+ -- --conf spark.sql.shuffle.partitions=400

# selective refresh — selectors resolve to dataset names:
lfd refresh --select order_events+          # incremental update of a subgraph
lfd run --refresh 'order_events,tag:gold'   # same, via run
lfd run --full-refresh 'order_events+'      # reset + recompute
lfd run --full-refresh-all
```

`--refresh`/`--full-refresh` take the same selectors as `--select` (names,
`+a`, `a+`, `tag:x`, `resource_type:seed`) — the spec is compiled in full so
every dataset name resolves, and the flags become
`spark-pipelines run --refresh ds1,ds2`.

## What the pipeline looks like

**One `spark-pipeline.yml` = one pipeline.** Everything a project compiles —
every MV, streaming table, view, and flow — belongs to a single pipeline
named after the project (`jaffle_lake` here). In the Databricks UI it
appears under **Lakeflow → Pipelines** as that one entry; opening it shows
the DAG: **datasets are nodes, flows are edges**.

For the example project the graph looks like:

```
raw.orders ──STREAM──> stg_orders ─┬─> order_events ═flow═> order_events (ST)
raw.orders_eu ──STREAM─────────────┘   order_events_eu ═flow═> order_events (ST)
stg_customers ─────────────────────────> customers (MV)
order_events ──────────────────────────> daily_revenue (MV)
raw.customers_feed ─> customers_scd2__cdc ═AUTO CDC═> customers_scd2 (ST)
raw.inventory ═AUTO CDC snapshot═> inventory_snapshot (ST)
seeds/order_status_map.csv ────────────> order_status_map (MV)
```

(`order_item_totals` is `ephemeral` — inlined as a CTE, no node. Temp staging
views like `customers_scd2__cdc` show as intermediate nodes feeding their
flow.)

## Deploying with Databricks Asset Bundles

`lfd` emits exactly what a DAB `pipelines` resource points at. The example
ships a `databricks.yml`:

```bash
lfd compile                          # target/definitions/*.sql + spark-pipeline.yml
databricks bundle deploy             # creates ONE pipeline named `jaffle_lake`
databricks bundle run jaffle_lake    # triggers a pipeline update
```

After deploy, the pipeline appears in **Lakeflow → Pipelines** as
`jaffle_lake` with the full DAG above. Selective refreshes map to update
triggers on that pipeline (`spark-pipelines run --refresh ...`, or a
bundle run with the same flags).

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
  Refreshes are the selective operation: `--refresh`/`--full-refresh` pick the
  datasets to update inside a fully compiled spec.

## Tests

```bash
python3 -m pytest tests -q
```
