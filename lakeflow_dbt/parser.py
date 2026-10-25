"""Parse .sql model files (Jinja + dbt-flavored globals) and schema.yml files."""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml
from jinja2 import Environment, FileSystemLoader, StrictUndefined
from jinja2.exceptions import TemplateError

from .errors import ConfigError, ParseError
from .model import (
    NAME_RE,
    ColumnTest,
    Model,
    ModelMeta,
    Project,
    SourceTable,
)
from .project import model_config_for
from .seeds import load_seeds

REF_MARKER = '__LFD_REF__("{}")'
SRC_MARKER = '__LFD_SRC__("{}")'
_REF_RE = re.compile(r'__LFD_REF__\("([^"]+)"\)')
_SRC_RE = re.compile(r'__LFD_SRC__\("([^"]+)"\)')


@dataclass
class ParseResult:
    models: Dict[str, Model] = field(default_factory=dict)
    sources: Dict[str, SourceTable] = field(default_factory=dict)  # key: "source.table"
    model_meta: Dict[str, ModelMeta] = field(default_factory=dict)
    warnings: List[str] = field(default_factory=list)


# --------------------------------------------------------------------------
# Jinja context
# --------------------------------------------------------------------------

class _ModelContext:
    """Captures ref()/source()/config()/var() calls made while rendering a model."""

    def __init__(self, model_name: str, project_vars: Dict[str, Any]):
        self.model_name = model_name
        self.vars = project_vars
        self.refs: List[str] = []
        self.sources: List[str] = []
        self.config_kwargs: Dict[str, Any] = {}

    def ref(self, name: str) -> str:
        if name not in self.refs:
            self.refs.append(name)
        return REF_MARKER.format(name)

    def source(self, source_name: str, table_name: Optional[str] = None) -> str:
        if table_name is None:
            raise ParseError(
                f"{self.model_name}: source() requires two args: source('name', 'table')"
            )
        key = f"{source_name}.{table_name}"
        if key not in self.sources:
            self.sources.append(key)
        return SRC_MARKER.format(key)

    def config(self, **kwargs) -> str:
        self.config_kwargs.update(kwargs)
        return ""

    def var(self, name: str, default: Any = None) -> Any:
        return self.vars.get(name, default)

    def env_var(self, name: str, default: Any = None) -> Any:
        import os

        return os.environ.get(name, default)


def _jinja_env(project: Project) -> Environment:
    search = [str(project.root / p) for p in project.model_paths]
    search += [str(project.root / p) for p in project.macro_paths]
    return Environment(
        loader=FileSystemLoader(search),
        undefined=StrictUndefined,
        keep_trailing_newline=True,
    )


def _render_model(project: Project, path: Path, model_root: Path) -> Model:
    name = path.stem
    if not NAME_RE.match(name):
        raise ParseError(f"{path}: model name {name!r} must match {NAME_RE.pattern}")
    raw = path.read_text()
    ctx = _ModelContext(name, project.vars)
    env = _jinja_env(project)
    env.globals.update(ref=ctx.ref, source=ctx.source, config=ctx.config, var=ctx.var,
                       env_var=ctx.env_var)
    try:
        rendered = env.from_string(raw).render()
    except TemplateError as e:
        raise ParseError(f"{path}: {e}") from e

    fqn_dir = ""
    try:
        fqn_dir = path.parent.relative_to(model_root).as_posix()
    except ValueError:
        pass
    try:
        config = model_config_for(project, fqn_dir, ctx.config_kwargs)
    except ConfigError as e:
        raise ParseError(f"{path}: {e}") from e

    return Model(
        name=name,
        rel_path=path.relative_to(project.root),
        abs_path=path,
        raw_sql=raw,
        rendered_sql=rendered,
        refs=list(ctx.refs),
        sources=list(ctx.sources),
        config=config,
        fqn_dir=fqn_dir,
    )


# --------------------------------------------------------------------------
# Python models — dbt-style `def model(dbt, spark)` files compiled to
# pyspark.pipelines (dp) definitions.
# --------------------------------------------------------------------------

_PY_REF_RE = re.compile(r"dbt\.ref\(\s*['\"]([A-Za-z_][\w]*)['\"]\s*\)")
_PY_SRC_RE = re.compile(
    r"dbt\.source\(\s*['\"]([A-Za-z_][\w]*)['\"]\s*,\s*['\"]([A-Za-z_][\w]*)['\"]\s*\)"
)


def _py_literal(node, where: str):
    import ast

    try:
        return ast.literal_eval(node)
    except (ValueError, SyntaxError):
        raise ParseError(f"{where}: config values must be literals")


def _render_py_model(project: Project, path: Path, model_root: Path) -> Model:
    """Parse a python model: `def model(dbt, spark)` whose body builds a
    DataFrame. `dbt.config(**kw)` sets config; `dbt.ref('x')` /
    `dbt.source('s','t')` mark dependencies and are rewritten to name strings.
    """
    import ast

    name = path.stem
    if not NAME_RE.match(name):
        raise ParseError(f"{path}: model name {name!r} must match {NAME_RE.pattern}")
    raw = path.read_text()
    try:
        tree = ast.parse(raw)
    except SyntaxError as e:
        raise ParseError(f"{path}: invalid python: {e}") from e

    fn = next(
        (n for n in ast.walk(tree)
         if isinstance(n, ast.FunctionDef) and n.name == "model"),
        None,
    )
    if fn is None:
        raise ParseError(
            f"{path}: python models must define `def model(dbt, spark)`"
        )

    config_kwargs: Dict[str, Any] = {}
    refs: List[str] = []
    sources: List[str] = []
    config_lines = set()
    lines = raw.splitlines()

    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        attr = getattr(func, "attr", None)
        base = getattr(getattr(func, "value", None), "id", None)
        if base != "dbt" or attr not in ("config", "ref", "source"):
            continue
        if attr == "config":
            config_lines.update(range(node.lineno, (node.end_lineno or node.lineno) + 1))
            for kw in node.keywords:
                config_kwargs[kw.arg] = _py_literal(kw.value, f"{path}")
        elif attr == "ref":
            if len(node.args) != 1 or not isinstance(node.args[0], ast.Constant):
                raise ParseError(f"{path}: dbt.ref() takes one string arg")
            rname = node.args[0].value
            if rname not in refs:
                refs.append(rname)
        else:  # source
            if len(node.args) != 2 or not all(
                isinstance(a, ast.Constant) for a in node.args
            ):
                raise ParseError(f"{path}: dbt.source() takes two string args")
            key = f"{node.args[0].value}.{node.args[1].value}"
            if key not in sources:
                sources.append(key)

    # body = function body minus signature and dbt.config(...) statements,
    # with dbt.ref/dbt.source replaced by plain name strings
    body_lines = lines[fn.body[0].lineno - 1:(fn.body[-1].end_lineno or fn.body[-1].lineno)]
    body = "\n".join(
        ln for i, ln in enumerate(body_lines, start=fn.body[0].lineno)
        if i not in config_lines
    )
    import textwrap
    body = _PY_SRC_RE.sub(lambda m: repr(f"{m.group(1)}.{m.group(2)}"), body)
    body = _PY_REF_RE.sub(lambda m: repr(m.group(1)), body)
    body = textwrap.dedent(body)

    fqn_dir = ""
    try:
        fqn_dir = path.parent.relative_to(model_root).as_posix()
    except ValueError:
        pass
    try:
        config = model_config_for(project, fqn_dir, config_kwargs)
    except ConfigError as e:
        raise ParseError(f"{path}: {e}") from e

    return Model(
        name=name,
        rel_path=path.relative_to(project.root),
        abs_path=path,
        raw_sql=raw,
        rendered_sql=body,
        refs=refs,
        sources=sources,
        config=config,
        fqn_dir=fqn_dir,
        language="python",
    )


# --------------------------------------------------------------------------
# schema.yml
# --------------------------------------------------------------------------

def _parse_tests(raw_tests: Any, column: Optional[str], where: str) -> List[ColumnTest]:
    out = []
    for t in raw_tests or []:
        if isinstance(t, str):
            out.append(ColumnTest(test_name=t, column=column))
        elif isinstance(t, dict):
            if len(t) != 1:
                raise ParseError(f"{where}: invalid test entry {t!r}")
            tname, targs = next(iter(t.items()))
            targs = dict(targs or {})
            cfg = targs.pop("config", {}) or {}
            severity = cfg.get("on_violation") or cfg.get("severity")
            out.append(ColumnTest(test_name=tname, column=column, args=targs,
                                  severity=severity))
        else:
            raise ParseError(f"{where}: invalid test entry {t!r}")
    return out


def _parse_schema_file(path: Path, result: ParseResult) -> None:
    try:
        data = yaml.safe_load(path.read_text()) or {}
    except yaml.YAMLError as e:
        raise ParseError(f"{path}: invalid YAML: {e}") from e

    for src in data.get("sources") or []:
        sname = src.get("name")
        if not sname:
            raise ParseError(f"{path}: a source is missing `name`")
        for tbl in src.get("tables") or []:
            tname = tbl.get("name")
            if not tname:
                raise ParseError(f"{path}: a source table in {sname} is missing `name`")
            key = f"{sname}.{tname}"
            result.sources[key] = SourceTable(
                name=tname,
                source=sname,
                catalog=tbl.get("catalog", src.get("catalog")),
                schema=tbl.get("schema", src.get("schema")),
                streaming=bool(tbl.get("streaming", src.get("streaming", False))),
                description=tbl.get("description"),
                identifier=tbl.get("identifier"),
            )

    for m in data.get("models") or []:
        mname = m.get("name")
        if not mname:
            raise ParseError(f"{path}: a model entry is missing `name`")
        meta = result.model_meta.setdefault(mname, ModelMeta(name=mname))
        if m.get("description"):
            meta.description = m["description"]
        if m.get("config"):
            meta.config.update(m["config"])
        for col in m.get("columns") or []:
            cname = col.get("name")
            if not cname:
                raise ParseError(f"{path}: a column of model {mname} is missing `name`")
            where = f"{path}: model {mname}, column {cname}"
            col_tests = _parse_tests(col.get("tests") or col.get("data_tests"),
                                     cname, where)
            meta.tests.extend(col_tests)
            meta.columns[cname] = {
                "description": col.get("description"),
                "tests": [t.test_name for t in col_tests],
            }
        where = f"{path}: model {mname}"
        meta.tests.extend(_parse_tests(m.get("tests") or m.get("data_tests"), None, where))


# --------------------------------------------------------------------------
# Entry point
# --------------------------------------------------------------------------

def parse_project(project: Project) -> ParseResult:
    result = ParseResult()

    missing_paths = []
    for mpath in project.model_paths:
        mroot = project.root / mpath
        if not mroot.is_dir():
            missing_paths.append(mpath)
            continue
        for path in sorted(mroot.rglob("*.sql")):
            model = _render_model(project, path, mroot)
            if model.name in result.models:
                raise ParseError(
                    f"duplicate model name {model.name!r}: {path} and "
                    f"{result.models[model.name].abs_path}"
                )
            result.models[model.name] = model
        for path in sorted(mroot.rglob("*.py")):
            model = _render_py_model(project, path, mroot)
            if model.name in result.models:
                raise ParseError(
                    f"duplicate model name {model.name!r}: {path} and "
                    f"{result.models[model.name].abs_path}"
                )
            result.models[model.name] = model
        for path in sorted(mroot.rglob("*.yml")) + sorted(mroot.rglob("*.yaml")):
            if path.name in PROJECT_FILES_HINTS:
                continue
            _parse_schema_file(path, result)

    for seed in load_seeds(project):
        if seed.name in result.models:
            raise ParseError(
                f"duplicate node name {seed.name!r}: {seed.abs_path} and "
                f"{result.models[seed.name].abs_path}"
            )
        result.models[seed.name] = seed

    if not result.models:
        raise ConfigError(
            "no models or seeds found; looked in model-paths "
            f"{project.model_paths} (missing: {missing_paths or 'none'}) "
            f"and seed-paths {project.seed_paths}"
        )

    # apply schema.yml metadata to models
    for mname, meta in result.model_meta.items():
        model = result.models.get(mname)
        if model is None:
            result.warnings.append(f"schema.yml documents model {mname!r} but no such model exists")
            continue
        model.description = meta.description
        model.tests = list(meta.tests)
        model.columns = dict(meta.columns)
        if meta.config:
            try:
                model.config = model.config.merged(meta.config)
            except ValueError as e:
                raise ParseError(f"schema.yml config for {mname!r}: {e}") from e

    # flow-only models: `into` must name another model; add it as a dependency
    # edge so compile closure always emits the target's CREATE
    by_dataset = {m.dataset_name: m.name for m in result.models.values()}
    for m in result.models.values():
        if not m.config.into:
            continue
        target = m.config.into
        if target == m.name or target == m.dataset_name:
            raise ParseError(f"{m.name}: `into` cannot target itself")
        if target in result.models or target in by_dataset:
            target_model = result.models.get(target) or result.models[by_dataset[target]]
            if target_model.name not in m.refs:
                m.refs.append(target_model.name)
        elif "." not in target:
            raise ParseError(
                f"{m.name}: into={target!r} names no model — pass a model name "
                f"or a qualified external target like 'catalog.schema.table'"
            )
        # qualified into= -> external sink table outside this project

    return result


PROJECT_FILES_HINTS = {"lakeflow_project.yml", "lakeflow_project.yaml",
                       "dbt_project.yml", "dbt_project.yaml"}
