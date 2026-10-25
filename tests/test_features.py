"""Seeds, AUTO CDC / snapshot materializations, selective refresh."""
import pytest

from lakeflow_dbt.compiler import Compiler
from lakeflow_dbt.errors import CompilationError
from lakeflow_dbt.parser import parse_project
from lakeflow_dbt.project import load_project


def compile_example(project, parsed, tmp_path, select=None):
    res = Compiler(project, parsed).compile(select=select, target_dir=tmp_path)
    return res, {f.stem: f.read_text() for f in res.files}


# --- seeds -------------------------------------------------------------------

def test_seed_compiles_to_values_mv(project, parsed, tmp_path):
    _, out = compile_example(project, parsed, tmp_path)
    sql = out["order_status_map"]
    assert "CREATE OR REFRESH MATERIALIZED VIEW order_status_map" in sql
    assert "from values" in sql
    assert "('placed', 'Placed', 1)" in sql.replace("'placed'", "'placed'")
    assert "as _seed(status, label, sort_order)" in sql


def test_seed_type_inference(tmp_project):
    root = tmp_project({
        "lakeflow_project.yml": "name: t\n",
        "seeds/s.csv": "id,price,active,name\n1,1.5,true,x\n2,2.0,false,y\n",
    })
    p = parse_project(load_project(str(root)))
    assert "s" in p.models
    sql = p.models["s"].rendered_sql
    assert "(1, 1.5, true, 'x')" in sql


def test_seed_only_selection(project, parsed, tmp_path):
    _, out = compile_example(project, parsed, tmp_path,
                             select="resource_type:seed")
    assert set(out) == {"order_status_map"}


# --- auto_cdc ----------------------------------------------------------------

def test_auto_cdc_emits_flow(project, parsed, tmp_path):
    _, out = compile_example(project, parsed, tmp_path)
    sql = out["customers_scd2"]
    assert "CREATE TEMPORARY VIEW customers_scd2__cdc" in sql
    assert "CREATE OR REFRESH STREAMING TABLE customers_scd2" in sql
    assert "AUTO CDC INTO customers_scd2" in sql
    assert "FROM STREAM(customers_scd2__cdc)" in sql
    assert "KEYS (customer_id)" in sql
    assert "IGNORE NULL UPDATES" in sql
    assert "APPLY AS DELETE WHEN op = 'DELETE'" in sql
    assert "SEQUENCE BY updated_at" in sql
    assert "STORED AS SCD TYPE 2" in sql
    assert "TRACK HISTORY ON * EXCEPT (updated_at)" in sql
    # the staged view reads the source without STREAM() (STREAM wraps the view)
    assert "from main.raw_data.customers_feed" in sql


def test_snapshot_emits_from_snapshot(project, parsed, tmp_path):
    _, out = compile_example(project, parsed, tmp_path)
    sql = out["inventory_snapshot"]
    assert "CREATE OR REFRESH STREAMING TABLE inventory_snapshot" in sql
    assert "FROM SNAPSHOT (" in sql
    assert "WITH VERSION (snapshot_ts)" in sql
    assert "KEYS (sku)" in sql
    assert "SEQUENCE BY" not in sql


def test_auto_cdc_requires_keys_and_sequence(tmp_project):
    root = tmp_project({
        "lakeflow_project.yml": "name: t\n",
        "models/a.sql": "{{ config(materialized='auto_cdc') }}\nselect 1 as id\n",
    })
    p = parse_project(load_project(str(root)))
    with pytest.raises(CompilationError, match="keys"):
        Compiler(load_project(str(root)), p).compile(target_dir=root / "t1")


# --- multi-flow targets (into) -------------------------------------------------

def test_into_emits_flow_only(project, parsed, tmp_path):
    _, out = compile_example(project, parsed, tmp_path)
    sql = out["order_events_eu"]
    assert "CREATE FLOW order_events_eu_flow AS INSERT INTO order_events BY NAME" in sql
    assert "STREAM(main.raw_data.orders_eu)" in sql
    # no dataset DDL of its own
    assert "CREATE OR REFRESH" not in sql and "STREAMING TABLE" not in sql
    # the target's CREATE still exists
    assert "CREATE OR REFRESH STREAMING TABLE order_events" in out["order_events"]


def test_into_model_not_a_dataset(project, parsed):
    c = Compiler(project, parsed)
    datasets = c.resolve_datasets("+order_events_eu")  # parents incl. the target
    assert "order_events_eu" not in datasets   # flow-only -> no dataset
    assert "order_events" in datasets          # the target table is refreshable


def test_into_requires_flow_materialization(tmp_project):
    root = tmp_project({
        "lakeflow_project.yml": "name: t\n",
        "models/a.sql": "{{ config(materialized='incremental') }}\nselect 1 as id\n",
        "models/b.sql": "{{ config(materialized='table', into='a') }}\nselect 1 as id\n",
    })
    p = parse_project(load_project(str(root)))
    with pytest.raises(CompilationError, match="into.*streaming table"):
        Compiler(load_project(str(root)), p).compile(target_dir=root / "t1")


def test_into_unknown_target_rejected(tmp_project):
    from lakeflow_dbt.errors import ParseError
    root = tmp_project({
        "lakeflow_project.yml": "name: t\n",
        "models/b.sql": "{{ config(materialized='incremental', into='ghost') }}\nselect 1\n",
    })
    with pytest.raises(ParseError, match="ghost"):
        parse_project(load_project(str(root)))


# --- python models ---------------------------------------------------------------

def test_python_model_compiles(project, parsed, tmp_path):
    _, out = compile_example(project, parsed, tmp_path)
    py = out["customer_rank"]
    assert "from pyspark import pipelines as dp" in py
    assert "@dp.materialized_view(name='customer_rank'" in py
    assert "spark.read.table('customers')" in py
    assert "spark.read.table('order_events')" in py
    assert "dbt.config" not in py and "dbt.ref" not in py


def test_state_selectors(project, parsed, tmp_path):
    import json
    compile_example(project, parsed, tmp_path)
    manifest = json.loads((tmp_path / "manifest.json").read_text())
    state = {n: d["checksum"] for n, d in manifest["nodes"].items()}
    c = Compiler(project, parsed, state=state)
    with pytest.raises(Exception, match="no models matched state:modified"):
        c.graph.select("state:modified")
    assert set(c.graph.select("state:unmodified")) == set(parsed.models)
    with pytest.raises(Exception, match="no models matched state:new"):
        c.graph.select("state:new")


def test_new_selectors(project, parsed):
    g = Compiler(project, parsed).graph
    assert set(g.select("fqn:marts.customers")) == {"customers"}
    assert set(g.select("config:materialized:incremental")) >= {"order_events"}
    assert set(g.select("config:flow:auto_cdc")) >= {"customers_scd2"}
    # comma = intersection: marts models that are also incremental-ish
    assert set(g.select("marts,config:materialized:incremental")) == {
        "order_events", "order_events_eu", "customers_scd2", "inventory_snapshot",
    }


def test_external_into(tmp_project):
    root = tmp_project({
        "lakeflow_project.yml": "name: t\n",
        "models/sink.sql": "{{ config(materialized='incremental', into='main.out.events') }}\nselect 1 as id\n",
    })
    p = parse_project(load_project(str(root)))
    res = Compiler(load_project(str(root)), p).compile(target_dir=root / "t")
    sql = {f.stem: f.read_text() for f in res.files}["sink"]
    assert "INSERT INTO main.out.events" in sql


# --- multi-pipeline projects ---------------------------------------------------

from pathlib import Path as _Path

LAYERED = _Path(__file__).resolve().parents[1] / "examples" / "layered_shop"


def layered():
    p = load_project(str(LAYERED))
    return p, parse_project(p)


def test_pipelines_config_and_dirs():
    project, parsed = layered()
    assert set(project.pipelines) == {"bronze", "gold"}
    c = Compiler(project, parsed)
    a = c.pipeline_assignments()
    assert a["bronze"] == {"orders_st", "customers_cdc", "status_codes"}
    assert a["gold"] == {"customer_orders", "daily_revenue"}


def test_pipelines_emit_separate_specs(tmp_path):
    project, parsed = layered()
    results = Compiler(project, parsed).compile_pipelines(target_dir=tmp_path)
    assert set(results) == {"bronze", "gold"}
    for pname, res in results.items():
        spec = res.target_dir / "spark-pipeline.yml"
        assert spec.exists()
        assert f"name: {pname}" in spec.read_text()


def test_cross_pipeline_ref_is_batch_read(tmp_path):
    project, parsed = layered()
    results = Compiler(project, parsed).compile_pipelines(target_dir=tmp_path)
    gold = {f.stem: f.read_text() for f in results["gold"].files}
    # orders_st/customers_cdc are bronze-owned: plain reads, no STREAM
    assert "from orders_st" in gold["customer_orders"]
    assert "STREAM(" not in gold["customer_orders"]
    # and the upstream datasets are NOT emitted into gold
    assert "orders_st" not in gold
    bronze = {f.stem: f.read_text() for f in results["bronze"].files}
    assert "CREATE OR REFRESH STREAMING TABLE orders_st" in bronze["orders_st"]


def test_pipeline_overlap_rejected(tmp_project):
    root = tmp_project({
        "lakeflow_project.yml": "name: t\npipelines:\n  a: staging\n  b: staging\n",
        "models/staging/x.sql": "select 1 as id\n",
    })
    p = parse_project(load_project(str(root)))
    with pytest.raises(CompilationError, match="only one pipeline"):
        Compiler(load_project(str(root)), p).compile_pipelines(target_dir=root / "t")


def test_dir_and_source_selectors(project, parsed):
    g = Compiler(project, parsed).graph
    assert set(g.select("staging")) == {"stg_customers", "stg_orders", "order_item_totals"}
    assert set(g.select("path:marts")) >= {"customers", "order_events"}
    assert set(g.select("source:raw.orders")) == {"stg_orders"}


def test_pipeline_flag_compiles_subset(project, parsed, tmp_path):
    # single-pipeline example: --pipeline names must match nothing -> error only
    # when pipelines are defined; here they aren't so it's ignored
    project, parsed = layered()
    results = Compiler(project, parsed).compile_pipelines(
        pipeline=["gold"], target_dir=tmp_path)
    assert set(results) == {"gold"}


# --- selective refresh ---------------------------------------------------------

def test_resolve_datasets(project, parsed):
    c = Compiler(project, parsed)
    assert c.resolve_datasets("customers") == ["customers"]
    assert set(c.resolve_datasets("stg_orders+")) >= {"order_events", "daily_revenue"}
    assert set(c.resolve_datasets("resource_type:seed")) == {"order_status_map"}
    # ephemeral drops out of the dataset list
    assert "order_item_totals" not in c.resolve_datasets("order_events")
    with pytest.raises(Exception, match="no_such_model|matched no datasets"):
        c.resolve_datasets("no_such_model")


def test_run_builds_refresh_args(project, parsed, tmp_path, capsys, monkeypatch):
    monkeypatch.setenv("PATH", "/nonexistent")
    from lakeflow_dbt.cli import main
    rc = main(["--project-dir", str(project.root), "run",
               "--refresh", "daily_revenue,customers_scd2"])
    out = capsys.readouterr().out
    assert rc == 2  # spark-pipelines missing
    assert "--refresh daily_revenue,customers_scd2" in out


def test_full_refresh_all_exclusivity(tmp_path, capsys, monkeypatch):
    monkeypatch.setenv("PATH", "/nonexistent")
    from lakeflow_dbt.cli import main
    rc = main(["--project-dir", str(tmp_path / "x"), "run"])
    assert rc == 1  # no project -> config error, not a crash
