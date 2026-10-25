import pytest

from lakeflow_dbt.errors import GraphError, ParseError
from lakeflow_dbt.parser import parse_project
from lakeflow_dbt.project import load_project

PROJ = """\
name: tproj
catalog: main
schema: tschema
"""

SCHEMA = """\
version: 2
sources:
  - name: raw
    schema: raw_data
    tables:
      - name: events
        streaming: true
"""


def test_example_project_parses(project, parsed):
    assert project.name == "jaffle_lake"
    assert set(parsed.models) == {
        "stg_customers", "stg_orders", "order_item_totals",
        "customers", "order_events", "daily_revenue",
        "customers_scd2", "inventory_snapshot", "order_status_map",
        "order_events_eu", "customer_rank",
    }
    assert parsed.models["order_status_map"].resource_type == "seed"
    assert parsed.sources["raw.orders"].streaming is True
    assert parsed.sources["raw.customers"].streaming is False


def test_materialization_defaults_from_tree(project, parsed):
    assert parsed.models["stg_orders"].config.materialized == "view"
    assert parsed.models["customers"].config.materialized == "table"
    assert parsed.models["order_events"].config.materialized == "incremental"
    assert parsed.models["order_item_totals"].config.materialized == "ephemeral"


def test_inline_config_and_var(tmp_project):
    root = tmp_project({
        "lakeflow_project.yml": PROJ + "vars:\n  limit: 10\n",
        "models/schema.yml": SCHEMA,
        "models/a.sql": (
            "{{ config(materialized='table', tags=['x']) }}\n"
            "select * from {{ source('raw','events') }} limit {{ var('limit') }}\n"
        ),
    })
    p = parse_project(load_project(str(root)))
    m = p.models["a"]
    assert m.config.materialized == "table"
    assert m.config.tags == ["x"]
    assert "limit 10" in m.rendered_sql


def test_unknown_ref_fails(tmp_project):
    root = tmp_project({
        "lakeflow_project.yml": PROJ,
        "models/a.sql": "select * from {{ ref('nope') }}\n",
    })
    p = parse_project(load_project(str(root)))
    from lakeflow_dbt.graph import Graph
    with pytest.raises(GraphError, match="nope"):
        Graph(p.models)


def test_undeclared_source_fails(tmp_project):
    root = tmp_project({
        "lakeflow_project.yml": PROJ,
        "models/a.sql": "select * from {{ source('raw','nope') }}\n",
    })
    p = parse_project(load_project(str(root)))
    from lakeflow_dbt.compiler import Compiler
    with pytest.raises(Exception, match="undeclared source"):
        Compiler(load_project(str(root)), p).compile()


def test_jinja_error_is_parse_error(tmp_project):
    root = tmp_project({
        "lakeflow_project.yml": PROJ,
        "models/a.sql": "select {{ missing_var }}\n",
    })
    with pytest.raises(ParseError):
        parse_project(load_project(str(root)))


def test_source_requires_two_args(tmp_project):
    root = tmp_project({
        "lakeflow_project.yml": PROJ,
        "models/a.sql": "select * from {{ source('raw') }}\n",
    })
    with pytest.raises(ParseError, match="two args"):
        parse_project(load_project(str(root)))
