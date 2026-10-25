"""Map dbt-style generic tests onto SDP row-level EXPECT constraints."""
from __future__ import annotations

from typing import Optional, Tuple

from .model import ColumnTest


def _literal(v) -> str:
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, (int, float)):
        return str(v)
    return "'" + str(v).replace("'", "''") + "'"


def _not_null(test: ColumnTest) -> str:
    return f"{test.column} IS NOT NULL"


def _accepted_values(test: ColumnTest) -> str:
    vals = test.args.get("values") or test.args.get("values_list") or []
    if not vals:
        raise ValueError("accepted_values requires `values`")
    return f"{test.column} IN ({', '.join(_literal(v) for v in vals)})"


def _expression_is_true(test: ColumnTest) -> str:
    expr = test.args.get("expression")
    if not expr:
        raise ValueError("expression_is_true requires `expression`")
    return f"({expr})"


def _accepted_range(test: ColumnTest) -> str:
    lo, hi = test.args.get("min_value"), test.args.get("max_value")
    parts = []
    if lo is not None:
        parts.append(f"{test.column} >= {_literal(lo)}")
    if hi is not None:
        parts.append(f"{test.column} <= {_literal(hi)}")
    if not parts:
        raise ValueError("accepted_range requires `min_value` and/or `max_value`")
    return " AND ".join(parts)


#: test name -> (test -> row-level SQL predicate). Row-level only: expectations
#: evaluate per row, so set-level checks (unique, relationships) can't map.
ROW_LEVEL_TESTS = {
    "not_null": _not_null,
    "accepted_values": _accepted_values,
    "expression_is_true": _expression_is_true,
    "accepted_range": _accepted_range,
}

#: set-level tests we know we cannot express as expectations.
UNEXPRESSIBLE_TESTS = {"unique", "relationships"}

_ON_VIOLATION_SQL = {"fail": "ON VIOLATION FAIL UPDATE", "drop": "ON VIOLATION DROP ROW"}


def compile_test(test: ColumnTest, default_violation: str) -> Tuple[Optional[str], Optional[str]]:
    """Return (constraint_sql, warning). Exactly one of the two is set."""
    if test.test_name in UNEXPRESSIBLE_TESTS:
        target = f" on column {test.column}" if test.column else ""
        return None, (
            f"test {test.test_name!r}{target} is a set-level check and cannot be "
            f"expressed as a pipeline expectation; skipped"
        )
    fn = ROW_LEVEL_TESTS.get(test.test_name)
    if fn is None:
        target = f" on column {test.column}" if test.column else ""
        return None, f"unknown test {test.test_name!r}{target}; skipped"
    if test.column is None and test.test_name != "expression_is_true":
        return None, f"test {test.test_name!r} needs a column; skipped"
    expr = fn(test)
    violation = test.severity or default_violation
    name = f"{test.test_name}_{test.column}" if test.column else test.test_name
    clause = _ON_VIOLATION_SQL.get(violation, "")
    return f"CONSTRAINT {name} EXPECT ({expr}) {clause}".strip(), None
