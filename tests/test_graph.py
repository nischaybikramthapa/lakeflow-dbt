import pytest

from lakeflow_dbt.errors import GraphError
from lakeflow_dbt.graph import Graph


def test_topo_order(parsed):
    g = Graph(parsed.models)
    order = g.topo_order()
    pos = {n: i for i, n in enumerate(order)}
    assert pos["stg_customers"] < pos["customers"]
    assert pos["stg_orders"] < pos["customers"]
    assert pos["stg_orders"] < pos["order_events"]
    assert pos["order_events"] < pos["daily_revenue"]


def test_select_plus_and_minus(parsed):
    g = Graph(parsed.models)
    assert set(g.select("+customers")) == {
        "customers", "stg_customers", "stg_orders",
    }
    assert set(g.select("stg_orders+")) == {
        "stg_orders", "customers", "order_events", "daily_revenue",
    }
    assert g.select("customers") == ["customers"]


def test_select_tag(parsed):
    g = Graph(parsed.models)
    assert g.select("tag:finance") == ["customers"]


def test_cycle_detected(tmp_project):
    root = tmp_project({
        "lakeflow_project.yml": "name: c\n",
        "models/a.sql": "select * from {{ ref('b') }}\n",
        "models/b.sql": "select * from {{ ref('a') }}\n",
    })
    from lakeflow_dbt.parser import parse_project
    from lakeflow_dbt.project import load_project
    p = parse_project(load_project(str(root)))
    with pytest.raises(GraphError, match="cycle"):
        Graph(p.models).topo_order()
