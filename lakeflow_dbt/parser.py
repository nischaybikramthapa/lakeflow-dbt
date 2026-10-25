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
            meta.tests.extend(_parse_tests(col.get("tests") or col.get("data_tests"),
                                           cname, where))
        where = f"{path}: model {mname}"
        meta.tests.extend(_parse_tests(m.get("tests") or m.get("data_tests"), None, where))


# --------------------------------------------------------------------------
# Entry point
# --------------------------------------------------------------------------

def parse_project(project: Project) -> ParseResult:
    result = ParseResult()

    for mpath in project.model_paths:
        mroot = project.root / mpath
        if not mroot.is_dir():
            raise ConfigError(f"model path {mroot} does not exist")
        for path in sorted(mroot.rglob("*.sql")):
            model = _render_model(project, path, mroot)
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

    # apply schema.yml metadata to models
    for mname, meta in result.model_meta.items():
        model = result.models.get(mname)
        if model is None:
            result.warnings.append(f"schema.yml documents model {mname!r} but no such model exists")
            continue
        model.description = meta.description
        model.tests = list(meta.tests)
        if meta.config:
            try:
                model.config = model.config.merged(meta.config)
            except ValueError as e:
                raise ParseError(f"schema.yml config for {mname!r}: {e}") from e

    return result


PROJECT_FILES_HINTS = {"lakeflow_project.yml", "lakeflow_project.yaml",
                       "dbt_project.yml", "dbt_project.yaml"}
