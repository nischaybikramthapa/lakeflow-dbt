"""`lfd test` — compile set-level dbt tests into standalone check queries.

Row-level checks ride inside dataset definitions as EXPECT constraints; the
set-level ones (unique, relationships, custom `expression_is_true` on the
model) can't — they compile here to plain SELECTs returning violations,
runnable with spark-sql or a notebook.
"""
from __future__ import annotations

import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from .model import ColumnTest, Model, Project
from .parser import ParseResult, _REF_RE


def _literal(v) -> str:
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, (int, float)):
        return str(v)
    return "'" + str(v).replace("'", "''") + "'"


def _unique_sql(t: ColumnTest, dataset: str) -> Optional[str]:
    if not t.column:
        return None
    return (
        f"-- unique({t.column}) on {dataset}\n"
        f"select {t.column}, count(*) as n\n"
        f"from {dataset}\ngroup by {t.column}\nhaving count(*) > 1;"
    )


def _relationships_sql(t: ColumnTest, dataset: str, parsed: ParseResult) -> Optional[str]:
    to = t.args.get("to") or t.args.get("model") or ""
    field = t.args.get("field") or t.args.get("to_field")
    m = re.search(r"ref\('([^']+)'\)", to)
    parent_name = m.group(1) if m else to
    parent = parsed.models.get(parent_name)
    if not t.column or not field or parent is None:
        return None
    pds = parent.qualified_name
    return (
        f"-- relationships({t.column} -> {pds}.{field}) on {dataset}\n"
        f"select c.{t.column}\n"
        f"from {dataset} c\n"
        f"left join {pds} p on c.{t.column} = p.{field}\n"
        f"where c.{t.column} is not null and p.{field} is null;"
    )


def _row_sql(t: ColumnTest, dataset: str) -> Optional[str]:
    """Row-level tests can also run as queries (redundant with EXPECT but
    useful for ad-hoc validation)."""
    from .tests_map import ROW_LEVEL_TESTS
    fn = ROW_LEVEL_TESTS.get(t.test_name)
    if fn is None:
        return None
    try:
        pred = fn(t)
    except ValueError:
        return None
    label = f"{t.test_name}({t.column})" if t.column else t.test_name
    return (
        f"-- {label} on {dataset} (rows violating the expectation)\n"
        f"select * from {dataset} where not ({pred});"
    )


def model_check_queries(model: Model, parsed: ParseResult) -> List[str]:
    if model.is_ephemeral:
        return []
    ds = model.qualified_name
    out: List[str] = []
    for t in model.tests:
        if t.test_name == "unique":
            q = _unique_sql(t, ds)
        elif t.test_name == "relationships":
            q = _relationships_sql(t, ds, parsed)
        else:
            q = _row_sql(t, ds)
        if q:
            out.append(q)
    return out


def generate(project: Project, parsed: ParseResult, select: Optional[str],
             target: Path) -> List[Path]:
    """Emit target/tests/<model>.sql for every model with checkable tests."""
    tdir = target / "tests"
    tdir.mkdir(parents=True, exist_ok=True)
    for stale in tdir.glob("*.sql"):
        stale.unlink()
    files: List[Path] = []
    for name, model in parsed.models.items():
        queries = model_check_queries(model, parsed)
        if not queries:
            continue
        f = tdir / f"{name}.sql"
        f.write_text(f"-- data-quality checks for {model.rel_path}\n\n"
                     + "\n\n".join(queries) + "\n")
        files.append(f)
    return files
