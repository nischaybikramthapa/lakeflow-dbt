import json

import yaml

from lakeflow_dbt.compiler import Compiler


def compile_example(project, parsed, tmp_path, select=None):
    c = Compiler(project, parsed)
    res = c.compile(select=select, target_dir=tmp_path)
    out = {f.stem: f.read_text() for f in res.files}
    return res, out


def test_view_emits_create_view(project, parsed, tmp_path):
    _, out = compile_example(project, parsed, tmp_path)
    # stg_customers feeds only batch models -> published view
    assert "CREATE VIEW stg_customers" in out["stg_customers"]
    assert "main.raw_data.customers" in out["stg_customers"]


def test_view_feeding_streaming_becomes_temp(project, parsed, tmp_path):
    res, out = compile_example(project, parsed, tmp_path)
    assert "CREATE TEMPORARY VIEW stg_orders" in out["stg_orders"]
    assert any("stg_orders" in w and "TEMPORARY VIEW" in w for w in res.warnings)


def test_table_emits_materialized_view(project, parsed, tmp_path):
    _, out = compile_example(project, parsed, tmp_path)
    mv = out["customers"]
    assert "CREATE OR REFRESH MATERIALIZED VIEW customers" in mv
    assert "CONSTRAINT not_null_customer_id EXPECT (customer_id IS NOT NULL)" in mv
    assert "ON VIOLATION DROP ROW" in mv  # marts +on_violation: drop
    assert "CLUSTER BY (customer_id)" in mv
    assert "'USD' as currency" in mv


def test_incremental_emits_streaming_table_and_flow(project, parsed, tmp_path):
    _, out = compile_example(project, parsed, tmp_path)
    st = out["order_events"]
    assert "CREATE OR REFRESH STREAMING TABLE order_events" in st
    assert "CREATE FLOW order_events_flow AS INSERT INTO order_events BY NAME" in st
    # streaming parent read via STREAM(); ephemeral CTE reads streaming source via STREAM too
    assert "STREAM(stg_orders)" in st
    assert "STREAM(main.raw_data.payments)" in st
    # ephemeral model inlined as CTE
    assert "WITH order_item_totals AS (" in st


def test_ephemeral_not_emitted(project, parsed, tmp_path):
    res, out = compile_example(project, parsed, tmp_path)
    assert "order_item_totals" not in out
    manifest = json.loads(res.manifest_file.read_text())
    assert manifest["nodes"]["order_item_totals"]["emitted"] is None


def test_expectations_and_unexpressible_test_warning(project, parsed, tmp_path):
    res, out = compile_example(project, parsed, tmp_path)
    # unique can't be an expectation -> warning
    assert any("unique" in w for w in res.warnings)
    # views can't carry expectations -> accepted_values on stg_orders is skipped
    assert "CONSTRAINT" not in out["stg_orders"]
    assert any("accepted_values" in w and "skipped" in w for w in res.warnings)
    # fail-level override on order_events.order_id
    assert "EXPECT (order_id IS NOT NULL) ON VIOLATION FAIL UPDATE" in out["order_events"]


def test_spec_and_manifest(project, parsed, tmp_path):
    res, _ = compile_example(project, parsed, tmp_path)
    spec = yaml.safe_load(res.spec_file.read_text())
    assert spec["name"] == "jaffle_lake"
    assert spec["catalog"] == "main"
    assert spec["database"] == "jaffle_lake"
    assert spec["libraries"][0]["glob"]["include"] == "definitions/**"
    assert spec["storage"].startswith("file://")
    manifest = json.loads(res.manifest_file.read_text())
    assert manifest["nodes"]["order_events"]["materialization"] == "streaming_table"
    assert manifest["sources"]["raw.orders"]["streaming"] is True


def test_select_limits_output(project, parsed, tmp_path):
    _, out = compile_example(project, parsed, tmp_path, select="customers")
    assert set(out) == {"customers", "stg_customers", "stg_orders"}


def test_batch_read_of_streaming_table_in_mv(project, parsed, tmp_path):
    _, out = compile_example(project, parsed, tmp_path)
    mv = out["daily_revenue"]
    # MVs read the streaming table's current snapshot, no STREAM()
    assert "from order_events" in mv
    assert "STREAM(order_events)" not in mv
