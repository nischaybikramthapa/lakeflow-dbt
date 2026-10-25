"""Core data model: projects, sources, models, tests."""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

#: dbt materialization -> the declarative dataset/flow it compiles to.
MATERIALIZATIONS = {
    "view": "view",
    "temp_view": "temporary_view",
    "temporary_view": "temporary_view",
    "table": "materialized_view",
    "incremental": "streaming_table",
    "ephemeral": "cte",
}
# Aliases accepted in {{ config(materialized=...) }}
MATERIALIZATION_ALIASES = {
    "materialized_view": "table",
    "mv": "table",
    "streaming_table": "incremental",
    "stream": "incremental",
}

VALID_VIOLATIONS = {"warn", "drop", "fail"}


@dataclass
class ModelConfig:
    """The subset of dbt config that maps onto a declarative pipeline."""

    materialized: str = "view"
    alias: Optional[str] = None
    schema: Optional[str] = None
    enabled: bool = True
    tags: List[str] = field(default_factory=list)
    tblproperties: Dict[str, str] = field(default_factory=dict)
    cluster_by: List[str] = field(default_factory=list)
    partition_cols: List[str] = field(default_factory=list)
    # default ON VIOLATION action for this model's tests: warn|drop|fail
    on_violation: str = "warn"
    private: bool = False

    def merged(self, other: Dict[str, Any]) -> "ModelConfig":
        """Return a copy with keys of ``other`` applied (dbt `+` config semantics)."""
        cfg = ModelConfig(**{**self.__dict__})
        unknown = []
        for k, v in (other or {}).items():
            k = k.lstrip("+")
            if k == "materialized":
                v = MATERIALIZATION_ALIASES.get(v, v)
                if v not in MATERIALIZATIONS:
                    unknown.append(f"materialized={v!r}")
                    continue
            if not hasattr(cfg, k):
                unknown.append(k)
                continue
            setattr(cfg, k, v)
        if unknown:
            raise ValueError(f"unknown config keys: {', '.join(sorted(unknown))}")
        if cfg.on_violation not in VALID_VIOLATIONS:
            raise ValueError(
                f"on_violation must be one of {sorted(VALID_VIOLATIONS)}, got {cfg.on_violation!r}"
            )
        return cfg


@dataclass
class ColumnTest:
    """One generic test on a column (or model-level test with column=None)."""

    test_name: str
    column: Optional[str] = None
    args: Dict[str, Any] = field(default_factory=dict)
    severity: Optional[str] = None  # override of model on_violation


@dataclass
class ModelMeta:
    """Everything schema.yml declares about a model."""

    name: str
    description: Optional[str] = None
    tests: List[ColumnTest] = field(default_factory=list)
    config: Dict[str, Any] = field(default_factory=dict)


@dataclass
class SourceTable:
    name: str
    source: str
    catalog: Optional[str] = None
    schema: Optional[str] = None
    streaming: bool = False
    description: Optional[str] = None
    identifier: Optional[str] = None  # physical table name when different

    @property
    def qualified_name(self) -> str:
        parts = [p for p in (self.catalog, self.schema, self.identifier or self.name) if p]
        return ".".join(parts)


@dataclass
class Model:
    """A single .sql model file, parsed and ready to compile."""

    name: str
    rel_path: Path          # relative to project root
    abs_path: Path
    raw_sql: str
    rendered_sql: str       # jinja-rendered, refs/sources as markers
    refs: List[str]
    sources: List[str]      # "source.table" keys
    config: ModelConfig
    fqn_dir: str = ""       # dir name under model-path root, for +materialized trees
    description: Optional[str] = None
    tests: List[ColumnTest] = field(default_factory=list)

    @property
    def dataset_name(self) -> str:
        return self.config.alias or self.name

    @property
    def qualified_name(self) -> str:
        return f"{self.config.schema}.{self.dataset_name}" if self.config.schema else self.dataset_name

    @property
    def materialization(self) -> str:
        return MATERIALIZATIONS[self.config.materialized]

    @property
    def is_ephemeral(self) -> bool:
        return self.materialization == "cte"


@dataclass
class Project:
    root: Path
    name: str
    catalog: Optional[str]
    schema: Optional[str]
    storage: Optional[str]
    model_paths: List[str]
    macro_paths: List[str]
    target_path: str
    vars: Dict[str, Any]
    configuration: Dict[str, Any]
    # raw `models:` tree from the project file, for +config resolution
    model_tree: Dict[str, Any]

    def base_config(self) -> ModelConfig:
        return ModelConfig()
