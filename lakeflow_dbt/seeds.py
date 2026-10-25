"""Seeds: CSV files that compile into self-contained declarative datasets."""
from __future__ import annotations

import csv
import io
import re
from pathlib import Path
from typing import List, Optional

from .errors import ParseError
from .model import NAME_RE, Model, Project
from .project import model_config_for

# beyond this many rows the VALUES literal gets unwieldy; warn and still emit
SIZE_WARN_ROWS = 5000


def _sanitize_col(name: str) -> str:
    c = re.sub(r"[^A-Za-z0-9_]", "_", name.strip())
    if not c or c[0].isdigit():
        c = "_" + c
    return c


def _infer_types(rows: List[List[str]], ncols: int) -> List[str]:
    types = []
    for i in range(ncols):
        vals = [r[i] for r in rows if i < len(r) and r[i] not in ("", "null", "NULL")]
        if not vals:
            types.append("string")
            continue
        try:
            for v in vals:
                int(v)
            types.append("int")
            continue
        except ValueError:
            pass
        try:
            for v in vals:
                float(v)
            types.append("double")
            continue
        except ValueError:
            pass
        if all(v.lower() in ("true", "false") for v in vals):
            types.append("boolean")
            continue
        types.append("string")
    return types


def _literal(v: str, typ: str) -> str:
    if v in ("", "null", "NULL"):
        return "NULL"
    if typ == "string":
        return "'" + v.replace("'", "''") + "'"
    if typ == "boolean":
        return "true" if v.lower() == "true" else "false"
    return v  # int/double: emit as-is


def csv_to_values_sql(text: str) -> str:
    """Turn a headered CSV into `SELECT * FROM VALUES (...) AS t(cols)`."""
    reader = csv.reader(io.StringIO(text))
    rows = [r for r in reader if any(c.strip() for c in r)]
    if not rows:
        raise ParseError("seed CSV has no rows")
    cols = [_sanitize_col(c) for c in rows[0]]
    if len(set(cols)) != len(cols):
        raise ParseError(f"seed CSV has duplicate columns after sanitizing: {cols}")
    data = rows[1:]
    types = _infer_types(data, len(cols))
    value_rows = ",\n    ".join(
        "(" + ", ".join(_literal(r[i] if i < len(r) else "", types[i])
                        for i in range(len(cols))) + ")"
        for r in data
    )
    if not data:
        # header-only seed: emit an empty typed select
        coldefs = ", ".join(f"cast(null as {t}) as {c}" for c, t in zip(cols, types))
        return f"select {coldefs} where 1 = 0"
    return (
        "select *\nfrom values\n    " + value_rows +
        f"\n    as { '_seed' }(" + ", ".join(cols) + ")"
    )


def load_seeds(project: Project) -> List[Model]:
    """Read seed-paths/**/*.csv into Model nodes whose SQL is a VALUES literal."""
    out: List[Model] = []
    for spath in project.seed_paths:
        sroot = project.root / spath
        if not sroot.is_dir():
            continue
        for path in sorted(sroot.rglob("*.csv")):
            name = path.stem
            if not NAME_RE.match(name):
                raise ParseError(
                    f"{path}: seed name {name!r} must match {NAME_RE.pattern}"
                )
            fqn_dir = ""
            try:
                fqn_dir = path.parent.relative_to(sroot).as_posix()
            except ValueError:
                pass
            text = path.read_text()
            try:
                sql = csv_to_values_sql(text)
            except ParseError as e:
                raise ParseError(f"{path}: {e}") from e
            try:
                config = model_config_for(
                    project, fqn_dir, {"materialized": "table"},
                    tree=(project.seed_tree or {}).get(project.name, {}),
                )
            except Exception as e:
                raise ParseError(f"{path}: {e}") from e
            out.append(Model(
                name=name,
                rel_path=path.relative_to(project.root),
                abs_path=path,
                raw_sql=text,
                rendered_sql=sql,
                refs=[],
                sources=[],
                config=config,
                fqn_dir=fqn_dir,
                resource_type="seed",
            ))
    return out
