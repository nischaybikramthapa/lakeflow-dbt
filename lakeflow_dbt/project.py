"""Loading lakeflow_project.yml (the dbt_project.yml analogue)."""
from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict, Optional

import yaml

from .errors import ConfigError
from .model import ModelConfig, Project

PROJECT_FILES = ("lakeflow_project.yml", "lakeflow_project.yaml", "dbt_project.yml", "dbt_project.yaml")


def find_project_root(start: Optional[str] = None) -> Path:
    """Walk up from ``start`` (or cwd) until a project file is found."""
    cur = Path(start or os.getcwd()).resolve()
    for candidate in (cur, *cur.parents):
        for fname in PROJECT_FILES:
            if (candidate / fname).is_file():
                return candidate
    raise ConfigError(
        f"no lakeflow_project.yml found at or above {cur}; run `lakeflow-dbt init` to create a project"
    )


def load_project(start: Optional[str] = None, vars_override: Optional[Dict[str, Any]] = None) -> Project:
    root = find_project_root(start)
    proj_file = next(p for p in (root / f for f in PROJECT_FILES) if p.is_file())
    try:
        raw = yaml.safe_load(proj_file.read_text()) or {}
    except yaml.YAMLError as e:
        raise ConfigError(f"{proj_file}: invalid YAML: {e}") from e
    if not isinstance(raw, dict):
        raise ConfigError(f"{proj_file}: expected a mapping at top level")

    name = raw.get("name")
    if not name:
        raise ConfigError(f"{proj_file}: missing required key `name`")

    merged_vars = dict(raw.get("vars") or {})
    merged_vars.update(vars_override or {})

    return Project(
        root=root,
        name=name,
        catalog=raw.get("catalog"),
        schema=raw.get("schema") or raw.get("database"),
        storage=raw.get("storage"),
        model_paths=list(raw.get("model-paths", raw.get("source-paths", ["models"]))),
        macro_paths=list(raw.get("macro-paths", ["macros"])),
        target_path=raw.get("target-path", "target"),
        vars=merged_vars,
        configuration=dict(raw.get("configuration") or {}),
        model_tree=raw.get("models") or {},
        seed_paths=list(raw.get("seed-paths", ["seeds"])),
        seed_tree=raw.get("seeds") or {},
        pipelines=_pipelines_map(raw.get("pipelines")),
    )


def _pipelines_map(raw) -> Dict[str, Dict[str, Any]]:
    """`pipelines:` -> {name: {select, catalog?, schema?, storage?,
    configuration?}}. Values may be a bare selector string or a mapping."""
    if not raw:
        return {}
    if not isinstance(raw, dict):
        raise ConfigError("`pipelines:` must be a mapping of name -> selector")
    out: Dict[str, Dict[str, Any]] = {}
    for pname, val in raw.items():
        if isinstance(val, str):
            out[pname] = {"select": val}
        elif isinstance(val, dict) and isinstance(val.get("select"), str):
            extra = {k: v for k, v in val.items() if k != "select"}
            bad = set(extra) - {"catalog", "schema", "database", "storage",
                                "configuration"}
            if bad:
                raise ConfigError(
                    f"pipelines.{pname}: unknown keys: {', '.join(sorted(bad))}"
                )
            out[pname] = {"select": val["select"], **extra}
        else:
            raise ConfigError(
                f"pipelines.{pname}: expected a selector string or {{select: ...}}"
            )
    return out


def dir_default_config(project: Project, fqn_dir: str,
                       tree: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Resolve a config tree for a subdirectory, dbt `+key` style.

    ``models:``/``seeds:`` keys without a ``+`` are directory names; ``+key``
    entries are configs applied to every node under that directory.
    """
    if tree is None:
        tree = project.model_tree.get(project.name, {}) or {}
    out: Dict[str, Any] = {}
    node: Dict[str, Any] = tree

    def absorb(d: Dict[str, Any]) -> None:
        for k, v in d.items():
            if k.startswith("+"):
                out[k[1:]] = v

    absorb(node)
    if fqn_dir:
        for part in Path(fqn_dir).parts:
            node = node.get(part)
            if not isinstance(node, dict):
                break
            absorb(node)
    return out


def model_config_for(project: Project, fqn_dir: str, inline: Dict[str, Any],
                     tree: Optional[Dict[str, Any]] = None) -> ModelConfig:
    """Final config for one node: defaults <- dir tree <- inline {{ config() }}."""
    base = project.base_config()
    try:
        return base.merged(dir_default_config(project, fqn_dir, tree)).merged(inline)
    except ValueError as e:
        raise ConfigError(f"{fqn_dir}: {e}") from e
