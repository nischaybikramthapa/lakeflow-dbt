# layered_shop — one dbt project, two deployable pipelines

Shows `pipelines:` in `lakeflow_project.yml`: models are grouped into layers,
each compiling to its own `spark-pipeline.yml` and deploying as a separate
pipeline in the Lakeflow UI.

```
models/
  ingestion/            # source-system layer
    orders_st.sql         incremental      STREAM(raw.orders) -> orders_st
    customers_cdc.sql     incremental+cdc  AUTO CDC raw.customers_feed -> customers_cdc
  marts/                # data product
    customer_orders.sql   table            join orders_st + customers_cdc (cross-pipeline reads)
    daily_revenue.sql     table            agg orders_st + status_codes (seed)
seeds/status_codes.csv

# lakeflow_project.yml
pipelines:
  bronze: "ingestion resource_type:seed"
  gold: marts
```

## Compile

```bash
lfd compile                # all pipelines
lfd compile --pipeline gold
```

produces

```
target/pipelines/bronze/definitions/*.sql + spark-pipeline.yml   (name: bronze)
target/pipelines/gold/definitions/*.sql   + spark-pipeline.yml   (name: gold)
```

## Deploy with DAB

`databricks.yml` maps each compiled spec to a `resources.pipelines` entry:

```bash
databricks bundle deploy     # creates layered_shop_bronze + layered_shop_gold
databricks bundle run layered_shop_gold
```

In **Lakeflow → Pipelines** you get two entries. `gold`'s DAG reads the
datasets `bronze` owns as plain tables — `STREAM()` is only emitted inside a
pipeline, so an incremental model can only `STREAM` from datasets in its own
pipeline. An MV reading another pipeline's streaming table (like
`customer_orders` → `customers_cdc`) reads its current snapshot, which is the
normal cross-pipeline pattern.

## Rules the compiler enforces

- A dataset selected by two `pipelines:` entries → compile error (one owner).
- A model upstream of a pipeline but in none → warning; it resolves as an
  external read.
- Selective refresh resolves per pipeline:
  `lfd run --refresh daily_revenue` passes `--refresh daily_revenue` only to
  the `gold` spec.
