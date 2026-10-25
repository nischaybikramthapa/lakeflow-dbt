"""Core data model: projects, sources, models, tests."""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

#: dbt materialization -> the declarative dataset it compiles to.
# A materialization is only ever a view, a temp view, a materialized view,
# or a streaming table — AUTO CDC is a *flow* on a streaming table, not a
# materialization (see ModelConfig.flow).
MATERIALIZATIONS = {
    "view": "view",
    "temp_view": "temporary_view",
    "temporary_view": "temporary_view",
    "table": "materialized_view",
    "incremental": "streaming_table",
    "ephemeral": "cte",
}
# Aliases accepted in {{ config(materialized=...) }} -> canonical materialized
MATERIALIZATION_ALIASES = {
    "materialized_view": "table",
    "mv": "table",
    "streaming_table": "incremental",
    "stream": "incremental",
    # legacy materializations -> incremental + a flow config (see below)
    "auto_cdc": "incremental",
    "apply_changes": "incremental",
    "snapshot": "incremental",
    "scd2": "incremental",
    "scd_type_2": "incremental",
}
# materialized values that imply a non-default flow config
MATERIALIZATION_FLOW_OVERRIDES = {
    "auto_cdc": {"flow": "auto_cdc"},
    "apply_changes": {"flow": "auto_cdc"},
    "snapshot": {"flow": "auto_cdc", "snapshot": True},
    "scd2": {"flow": "auto_cdc", "snapshot": True},
    "scd_type_2": {"flow": "auto_cdc", "snapshot": True},
}

VALID_FLOWS = {"append", "auto_cdc"}

#: stored_as value -> SQL
STORED_AS = {
    "scd_type_1": "SCD TYPE 1",
    "scd_type_2": "SCD TYPE 2",
    "scd1": "SCD TYPE 1",
    "scd2": "SCD TYPE 2",
    "bitemporal": "BITEMPORAL",
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
    # flow kind on a streaming table: 'append' (INSERT INTO ... BY NAME) or
    # 'auto_cdc' (AUTO CDC INTO / FROM SNAPSHOT). Not a materialization.
    flow: str = "append"
    snapshot: bool = False              # auto_cdc reads FROM SNAPSHOT (query)
    # AUTO CDC / snapshot flow options
    keys: List[str] = field(default_factory=list)
    sequence_by: Any = None            # str column or list -> STRUCT(...)
    stored_as: Optional[str] = None    # scd_type_1 | scd_type_2 | bitemporal
    track_history: Any = None          # list of cols, {'except': [...]}, or '*'
    columns: Any = None                # list of cols or {'except': [...]}
    apply_as_deletes: Optional[str] = None
    apply_as_truncates: Optional[str] = None
    ignore_null_updates: Any = None    # bool or column list
    version: Optional[str] = None      # snapshot only: WITH VERSION expression
    once: bool = False                 # AUTO CDC ONCE / one-time backfill
    # flow-only model: no dataset DDL, just an extra flow INTO another model's
    # streaming table (SDP supports several flows appending to one target)
    into: Optional[str] = None

    def merged(self, other: Dict[str, Any]) -> "ModelConfig":
        """Return a copy with keys of ``other`` applied (dbt `+` config semantics)."""
        cfg = ModelConfig(**{**self.__dict__})
        unknown = []
        for k, v in (other or {}).items():
            k = k.lstrip("+")
            if k == "materialized":
                overrides = MATERIALIZATION_FLOW_OVERRIDES.get(v, {})
                for ok, ov in overrides.items():
                    setattr(cfg, ok, ov)
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
        if cfg.flow not in VALID_FLOWS:
            raise ValueError(
                f"flow must be one of {sorted(VALID_FLOWS)}, got {cfg.flow!r}"
            )
        if cfg.flow != "append" and cfg.materialized not in (
            "incremental",
        ):
            raise ValueError(
                f"flow={cfg.flow!r} requires materialized='incremental' "
                f"(a streaming table), got {cfg.materialized!r}"
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
    # declared columns from schema.yml: name -> {description, tests}
    columns: Dict[str, Dict[str, Any]] = field(default_factory=dict)


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
    """A single model file, parsed and ready to compile."""

    name: str
    rel_path: Path          # relative to project root
    abs_path: Path
    raw_sql: str
    rendered_sql: str       # jinja-rendered (sql) or rewritten body (python)
    refs: List[str]
    sources: List[str]      # "source.table" keys
    config: ModelConfig
    fqn_dir: str = ""       # dir name under model-path root, for +materialized trees
    description: Optional[str] = None
    tests: List[ColumnTest] = field(default_factory=list)
    columns: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    resource_type: str = "model"   # 'model' | 'seed'
    language: str = "sql"          # 'sql' | 'python'

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

    @property
    def checksum(self) -> str:
        """Fingerprint for state: selection — source + resolved config + dir."""
        import hashlib
        import json
        h = hashlib.sha1()
        h.update(self.raw_sql.encode())
        h.update(json.dumps(self.config.__dict__, sort_keys=True,
                            default=str).encode())
        h.update(self.fqn_dir.encode())
        return h.hexdigest()


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
    seed_paths: List[str] = field(default_factory=list)
    # raw `seeds:` tree for +config resolution
    seed_tree: Dict[str, Any] = field(default_factory=dict)
    # `pipelines:` map — pipeline name -> {select, catalog?, schema?,
    # storage?, configuration?}. Empty = one pipeline containing the whole
    # project (each entry compiles to its own spec + deploys separately)
    pipelines: Dict[str, Dict[str, Any]] = field(default_factory=dict)

    def base_config(self) -> ModelConfig:
        return ModelConfig()
