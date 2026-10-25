"""Compile parsed dbt-style models into Lakeflow Declarative Pipeline SQL."""
from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Set

import yaml

from .errors import CompilationError
from .graph import Graph
from .model import Model, Project
from .parser import ParseResult, _REF_RE, _SRC_RE
from .tests_map import compile_test


@dataclass
class CompileResult:
    target_dir: Path
    spec_file: Path
    manifest_file: Path
    files: List[Path] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)


def _indent(sql: str, pad: str = "") -> str:
    text = sql.strip().rstrip(";")
    if pad:
        text = "\n".join(pad + ln if ln.strip() else ln for ln in text.splitlines())
    return text


def _comment_clause(comment: Optional[str]) -> str:
    if not comment:
        return ""
    return "COMMENT '" + comment.replace("'", "''") + "'"


def _tblproperties(props: Dict[str, str]) -> str:
    if not props:
        return ""
    inner = ", ".join(f"'{k}' = '{v}'" for k, v in props.items())
    return f"TBLPROPERTIES ({inner})"


def _table_clauses(model: Model, *, view: bool = False) -> str:
    parts = []
    if not view:
        if model.config.cluster_by:
            parts.append(f"CLUSTER BY ({', '.join(model.config.cluster_by)})")
        if model.config.partition_cols:
            parts.append(f"PARTITIONED BY ({', '.join(model.config.partition_cols)})")
    c = _comment_clause(model.description)
    if c:
        parts.append(c)
    tp = _tblproperties(model.config.tblproperties)
    if tp:
        parts.append(tp)
    return "\n".join(parts)


def _view_header_comment(model: Model) -> str:
    if model.description:
        return "-- " + model.description.replace("\n", "\n-- ") + "\n"
    return ""


class Compiler:
    def __init__(self, project: Project, parsed: ParseResult):
        self.project = project
        self.parsed = parsed
        self.graph = Graph(parsed.models)
        self.warnings: List[str] = list(parsed.warnings)
        # view-type models upstream of a streaming table: published views can't
        # be streaming sources, so they compile to TEMPORARY VIEW + STREAM() reads
        self._stream_views: Set[str] = set()

    # -- marker resolution -------------------------------------------------

    def _resolve_sql(self, sql: str, *, streaming: bool, ctes: Set[str]) -> str:
        """Replace ref/source markers with names (or STREAM() reads / CTE refs)."""

        def ref_sub(m: re.Match) -> str:
            name = m.group(1)
            model = self.parsed.models.get(name)
            if model is None:
                raise CompilationError(f"ref to unknown model {name!r}")
            if model.is_ephemeral:
                ctes.add(name)
                return model.dataset_name
            out = model.qualified_name
            if streaming and (
                model.materialization == "streaming_table" or name in self._stream_views
            ):
                return f"STREAM({out})"
            return out

        def src_sub(m: re.Match) -> str:
            key = m.group(1)
            st = self.parsed.sources.get(key)
            if st is None:
                raise CompilationError(
                    f"undeclared source {key!r} — declare it in schema.yml under `sources:`"
                )
            qname = st.qualified_name
            if not st.catalog and self.project.catalog:
                qname = f"{self.project.catalog}.{qname}" if st.schema else qname
            if streaming and st.streaming:
                return f"STREAM({qname})"
            return qname

        return _SRC_RE.sub(src_sub, _REF_RE.sub(ref_sub, sql))

    def _ephemeral_ctes(self, seeds: Set[str], *, streaming: bool) -> List[str]:
        """Transitive ephemeral deps, each resolved in the same read context."""
        seen: Set[str] = set()
        order: List[str] = []

        def visit(name: str) -> None:
            em = self.parsed.models[name]
            for r in em.refs:
                rm = self.parsed.models.get(r)
                if rm is not None and rm.is_ephemeral:
                    visit(r)
            if name not in seen:
                seen.add(name)
                order.append(name)

        for r in seeds:
            visit(r)

        ctes = []
        for name in order:
            em = self.parsed.models[name]
            inner: Set[str] = set()
            body = self._resolve_sql(em.rendered_sql, streaming=streaming, ctes=inner)
            # nested ephemeral refs inside inner resolve to CTE names declared
            # earlier in the same WITH clause (visit() is post-order DFS)
            ctes.append(f"{em.dataset_name} AS (\n{_indent(body, '    ')}\n)")
        return ctes

    def _body(self, model: Model, *, streaming: bool) -> str:
        ctes: Set[str] = set()
        sql = self._resolve_sql(model.rendered_sql, streaming=streaming, ctes=ctes)
        body = _indent(sql)
        if ctes:
            # all transitive ephemerals: each may be referenced by the model or
            # by another ephemeral declared in the same WITH clause
            blocks = self._ephemeral_ctes(ctes, streaming=streaming)
            if blocks:
                body = "WITH " + ",\n".join(blocks) + "\n\n" + body
        return body

    # -- expectation constraints -------------------------------------------

    def _constraints(self, model: Model) -> List[str]:
        clauses = []
        for t in model.tests:
            sql, warn = compile_test(t, model.config.on_violation)
            if sql:
                clauses.append(sql)
            if warn:
                self.warnings.append(f"{model.name}: {warn}")
        return clauses

    def _warn_dropped_tests(self, model: Model) -> None:
        for t in model.tests:
            self.warnings.append(
                f"{model.name}: test {t.test_name!r} skipped — "
                f"{model.materialization.replace('_', ' ')}s can't carry expectations; "
                f"use materialized='table' or 'incremental' to enforce it"
            )

    # -- statement emission -------------------------------------------------

    def _emit(self, model: Model) -> str:
        mat = model.materialization
        qname = model.qualified_name
        priv = "PRIVATE " if model.config.private else ""
        spec = ""
        if mat in ("materialized_view", "streaming_table"):
            constraints = self._constraints(model)
            if constraints:
                spec = " (\n  " + ",\n  ".join(constraints) + "\n)"
        clauses = _table_clauses(model)
        clauses = ("\n" + clauses) if clauses else ""
        header = f"-- {model.rel_path}\n"

        if mat in ("view", "temporary_view"):
            if model.config.cluster_by or model.config.partition_cols:
                self.warnings.append(
                    f"{model.name}: cluster_by/partition_cols ignored on views"
                )
        if mat == "view":
            body = self._body(model, streaming=False)
            self._warn_dropped_tests(model)
            if model.name in self._stream_views:
                return (
                    f"{header}{_view_header_comment(model)}"
                    f"CREATE TEMPORARY VIEW {qname}\nAS\n{body};\n"
                )
            return (
                f"{header}CREATE VIEW {qname}"
                f"{chr(10) + _table_clauses(model, view=True) if _table_clauses(model, view=True) else ''}"
                f"\nAS\n{body};\n"
            )
        if mat == "temporary_view":
            body = self._body(model, streaming=False)
            self._warn_dropped_tests(model)
            return (
                f"{header}{_view_header_comment(model)}"
                f"CREATE TEMPORARY VIEW {qname}\nAS\n{body};\n"
            )
        if mat == "materialized_view":
            body = self._body(model, streaming=False)
            return (
                f"{header}CREATE OR REFRESH {priv}MATERIALIZED VIEW {qname}{spec}{clauses}\n"
                f"AS\n{body};\n"
            )
        if mat == "streaming_table":
            create = (
                f"CREATE OR REFRESH {priv}STREAMING TABLE {qname}{spec}{clauses};"
            )
            body = self._body(model, streaming=True)
            flow = f"CREATE FLOW {model.dataset_name}_flow AS INSERT INTO {qname} BY NAME\n{body};"
            return header + create + "\n\n" + flow + "\n"
        raise CompilationError(f"{model.name}: cannot emit materialization {mat!r}")

    # -- public API ----------------------------------------------------------

    def compile(self, select: Optional[str] = None,
                target_dir: Optional[Path] = None) -> CompileResult:
        selected_names = self.graph.select(select)
        # compile closes over ancestors: the emitted pipeline must be
        # self-contained since datasets resolve by name at validation time
        order = self.graph.topo_order(selected_names)
        selected = set(order)
        for n in order:
            if self.parsed.models[n].materialization == "streaming_table":
                for a in self.graph.ancestors(n) & selected:
                    am = self.parsed.models[a]
                    if am.materialization in ("view", "temporary_view"):
                        if am.materialization == "view":
                            self.warnings.append(
                                f"{a}: 'view' feeds a streaming table; emitted as "
                                f"TEMPORARY VIEW (published views can't be streaming sources)"
                            )
                        self._stream_views.add(a)
        target = Path(target_dir or (self.project.root / self.project.target_path))
        defs = target / "definitions"
        defs.mkdir(parents=True, exist_ok=True)
        for stale in defs.glob("*.sql"):
            stale.unlink()

        files: List[Path] = []
        nodes = {}
        for name in order:
            model = self.parsed.models[name]
            if model.is_ephemeral:
                nodes[name] = _manifest_node(model, emitted=None)
                continue
            sql = self._emit(model)
            f = defs / f"{model.dataset_name}.sql"
            f.write_text(sql)
            files.append(f)
            nodes[name] = _manifest_node(model, emitted=str(f.relative_to(target)))

        spec = self._write_spec(target)
        manifest = self._write_manifest(target, nodes)
        self.warnings = sorted(set(self.warnings))
        return CompileResult(target_dir=target, spec_file=spec,
                             manifest_file=manifest, files=files,
                             warnings=self.warnings)

    def _write_spec(self, target: Path) -> Path:
        spec: Dict = {
            "name": self.project.name,
            "libraries": [{"glob": {"include": "definitions/**"}}],
        }
        storage = self.project.storage or f"file://{(target / 'storage').resolve()}"
        spec["storage"] = storage
        if self.project.catalog:
            spec["catalog"] = self.project.catalog
        if self.project.schema:
            spec["database"] = self.project.schema
        if self.project.configuration:
            spec["configuration"] = self.project.configuration
        f = target / "spark-pipeline.yml"
        f.write_text(yaml.safe_dump(spec, sort_keys=False, default_flow_style=False))
        return f

    def _write_manifest(self, target: Path, nodes: Dict) -> Path:
        sources = {k: {"catalog": s.catalog, "schema": s.schema,
                       "name": s.name, "identifier": s.identifier,
                       "streaming": s.streaming}
                   for k, s in self.parsed.sources.items()}
        manifest = {
            "project": self.project.name,
            "catalog": self.project.catalog,
            "schema": self.project.schema,
            "nodes": nodes,
            "sources": sources,
            "warnings": self.warnings,
        }
        import datetime

        manifest["generated_at"] = datetime.datetime.utcnow().isoformat() + "Z"
        f = target / "manifest.json"
        f.write_text(json.dumps(manifest, indent=2) + "\n")
        return f


def _manifest_node(model: Model, emitted: Optional[str]) -> Dict:
    return {
        "name": model.name,
        "dataset": model.qualified_name,
        "path": str(model.rel_path),
        "materialization": model.materialization,
        "refs": list(model.refs),
        "sources": list(model.sources),
        "emitted": emitted,
        "tags": model.config.tags,
    }
