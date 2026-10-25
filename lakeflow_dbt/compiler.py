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
from .model import STORED_AS, Model, Project
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


def _colspec(value, keyword: str) -> str:
    """`COLUMNS`/`TRACK HISTORY ON`/`IGNORE NULL UPDATES ON` clause bodies."""
    if value is None or value is False:
        return ""
    if value == "*" or value is True:
        return f"{keyword} *"
    if isinstance(value, dict) and "except" in value:
        return f"{keyword} * EXCEPT ({', '.join(value['except'])})"
    cols = value if isinstance(value, (list, tuple)) else [value]
    return f"{keyword} ({', '.join(str(c) for c in cols)})"


class Compiler:
    def __init__(self, project: Project, parsed: ParseResult,
                 state: Optional[Dict[str, str]] = None):
        self.project = project
        self.parsed = parsed
        self.graph = Graph(parsed.models, state=state)
        self.warnings: List[str] = list(parsed.warnings)
        # view-type models upstream of a streaming table: published views can't
        # be streaming sources, so they compile to TEMPORARY VIEW + STREAM() reads
        self._stream_views: Set[str] = set()
        # models emitted into the pipeline currently being compiled — a ref to
        # a model outside this set is a cross-pipeline read (plain table name,
        # never STREAM())
        self._emitted: Set[str] = set()

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
            out = self._dataset_ref(model)
            if streaming and name in self._emitted and (
                model.materialization == "streaming_table" or name in self._stream_views
            ):
                return f"STREAM({out})"
            if streaming and name not in self._emitted and (
                model.materialization == "streaming_table"
            ):
                self.warnings.append(
                    f"{model.name!r} is a streaming table in another pipeline; "
                    f"read inside a flow resolves to a batch read of {out}"
                )
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

    def _dataset_ref(self, model: Model) -> str:
        """Name readers see for a model: its `into` target when flow-only."""
        if model.config.into:
            target = self.parsed.models.get(model.config.into)
            if target is None:
                for other in self.parsed.models.values():
                    if other.dataset_name == model.config.into:
                        target = other
                        break
            if target is not None:
                return target.qualified_name
        return model.qualified_name

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
        if model.language == "python":
            if mat == "cte":
                raise CompilationError(
                    f"{model.name}: python models can't be materialized='ephemeral'"
                )
            return self._emit_python(model)
        if model.config.into and mat != "streaming_table":
            raise CompilationError(
                f"{model.name}: `into` requires a streaming table "
                f"(materialized='incremental'), got "
                f"materialized={model.config.materialized!r}"
            )
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
            if model.config.flow == "auto_cdc":
                return self._emit_auto_cdc(model, snapshot=model.config.snapshot,
                                           header=header, spec=spec,
                                           clauses=clauses)
            body = self._body(model, streaming=True)
            flow = f"CREATE FLOW {model.dataset_name}_flow AS INSERT INTO {qname} BY NAME\n{body};"
            if model.config.into:
                # flow-only: append into another model's streaming table
                target = self._into_target(model)
                self._warn_dropped_tests(model)
                flow = (f"CREATE FLOW {model.dataset_name}_flow AS "
                        f"INSERT INTO {target} BY NAME\n{body};")
                return header + flow + "\n"
            create = (
                f"CREATE OR REFRESH {priv}STREAMING TABLE {qname}{spec}{clauses};"
            )
            return header + create + "\n\n" + flow + "\n"
        raise CompilationError(f"{model.name}: cannot emit materialization {mat!r}")

    def _into_target(self, model: Model) -> str:
        into = model.config.into or ""
        if "." in into:
            return into   # qualified external target (sink table)
        for other in self.parsed.models.values():
            if other.name == into or other.dataset_name == into:
                return other.qualified_name
        raise CompilationError(f"{model.name}: into={into!r} names no model")

    # -- python emission ------------------------------------------------------

    def _py_expectations(self, model: Model) -> List[str]:
        """Row-level tests -> @dp.expect* decorators (expectations only)."""
        from .tests_map import ROW_LEVEL_TESTS, UNEXPRESSIBLE_TESTS
        deco = {"warn": "expect", "drop": "expect_or_drop",
                "fail": "expect_or_fail"}
        out: List[str] = []
        for t in model.tests:
            if t.test_name in UNEXPRESSIBLE_TESTS:
                self.warnings.append(
                    f"{model.name}: test {t.test_name!r} can't be a pipeline "
                    f"expectation; skipped"
                )
                continue
            fn = ROW_LEVEL_TESTS.get(t.test_name)
            if fn is None or (t.column is None and t.test_name != "expression_is_true"):
                self.warnings.append(f"{model.name}: test {t.test_name!r} skipped")
                continue
            try:
                expr = fn(t)
            except ValueError as e:
                self.warnings.append(f"{model.name}: {e}")
                continue
            dec = deco.get(t.severity or model.config.on_violation, "expect")
            tname = f"{t.test_name}_{t.column}" if t.column else t.test_name
            out.append(f'@dp.{dec}({tname!r}, {expr!r})')
        return out

    def _emit_python(self, model: Model) -> str:
        mat = model.materialization
        cfg = model.config
        qname = model.qualified_name
        body = _indent(model.rendered_sql, "    ")
        header = f"# {model.rel_path}\nfrom pyspark import pipelines as dp\n"
        comment = f', comment={model.description!r}' if model.description else ""
        decos = "\n".join(self._py_expectations(model))
        decos = (decos + "\n") if decos else ""

        if mat == "materialized_view":
            return (f"{header}\n@dp.materialized_view(name={model.dataset_name!r}{comment})\n"
                    f"{decos}def {model.dataset_name}(spark):\n{body}\n")
        if mat in ("view", "temporary_view"):
            if mat == "view":
                self.warnings.append(
                    f"{model.name}: python views emit as dp.temporary_view "
                    f"(the python API has no published-view decorator)"
                )
            self._warn_dropped_tests(model)
            return (f"{header}\n@dp.temporary_view\n"
                    f"def {model.dataset_name}(spark):\n{body}\n")
        if mat == "streaming_table":
            if cfg.flow == "auto_cdc":
                return self._emit_python_auto_cdc(model, header, body)
            flow = (f"@dp.append_flow(target={model.dataset_name!r})\n"
                    f"def {model.dataset_name}_flow(spark):\n{body}\n")
            if cfg.into:
                target = self._into_target(model)
                self._warn_dropped_tests(model)
                return (f"{header}\n@dp.append_flow(target={target!r})\n"
                        f"def {model.dataset_name}_flow(spark):\n{body}\n")
            return (f"{header}\ndp.create_streaming_table("
                    f"name={model.dataset_name!r}{comment})\n"
                    f"{decos}\n{flow}")
        raise CompilationError(
            f"{model.name}: python models support materialized="
            f"'table'|'incremental'|'view'|'temp_view', got "
            f"{model.config.materialized!r}"
        )

    def _emit_python_auto_cdc(self, model: Model, header: str, body: str) -> str:
        cfg = model.config
        if not cfg.keys:
            raise CompilationError(
                f"{model.name}: flow='auto_cdc' requires `keys`"
            )
        target = self._into_target(model) if cfg.into else model.qualified_name
        src_fn = f"{model.dataset_name}__cdc"
        sa = STORED_AS.get(str(cfg.stored_as).lower()) if cfg.stored_as else None
        if cfg.snapshot:
            if cfg.sequence_by:
                raise CompilationError(
                    f"{model.name}: `sequence_by` is not valid for snapshot"
                )
            kwargs = {"target": target, "source": src_fn, "keys": cfg.keys}
            if cfg.version:
                kwargs["version"] = cfg.version
            if sa:
                kwargs["stored_as"] = sa
            call = "dp.create_auto_cdc_from_snapshot_flow"
        else:
            if not cfg.sequence_by:
                raise CompilationError(
                    f"{model.name}: flow='auto_cdc' requires `sequence_by`"
                )
            seq = cfg.sequence_by
            kwargs = {"target": target, "source": src_fn, "keys": cfg.keys,
                      "sequence_by": (list(seq) if isinstance(seq, (list, tuple))
                                      else seq)}
            if cfg.apply_as_deletes:
                kwargs["apply_as_deletes"] = cfg.apply_as_deletes
            if cfg.apply_as_truncates:
                kwargs["apply_as_truncates"] = cfg.apply_as_truncates
            if cfg.ignore_null_updates:
                kwargs["ignore_null_updates"] = cfg.ignore_null_updates
            if sa:
                kwargs["stored_as"] = sa
            if cfg.once:
                kwargs["once"] = True
            call = "dp.create_auto_cdc_flow"
        args = ", ".join(f"{k}={v!r}" for k, v in kwargs.items())
        create = ("" if cfg.into else
                  f"dp.create_streaming_table(name={model.dataset_name!r})\n\n")
        return (f"{header}\n{create}"
                f"def {src_fn}(spark):\n{body}\n\n{call}({args})\n")

    def _emit_auto_cdc(self, model: Model, *, snapshot: bool,
                       header: str, spec: str, clauses: str) -> str:
        cfg = model.config
        qname = model.qualified_name
        priv = "PRIVATE " if cfg.private else ""
        if not cfg.keys:
            raise CompilationError(
                f"{model.name}: flow='auto_cdc' requires `keys` (e.g. keys=['id'])"
            )
        into = self._into_target(model) if cfg.into else qname
        once = "ONCE " if cfg.once else ""
        flow = [f"CREATE FLOW {model.dataset_name}_flow AS AUTO CDC {once}INTO {into}"]
        pre = ""
        if snapshot:
            body = self._body(model, streaming=False)
            if cfg.sequence_by:
                raise CompilationError(
                    f"{model.name}: `sequence_by` is not valid for snapshot; "
                    f"ordering comes from `version`"
                )
            flow.append(f"FROM SNAPSHOT (\n{_indent(body, '    ')}\n)")
            if cfg.version:
                flow.append(f"WITH VERSION ({cfg.version})")
        else:
            if not cfg.sequence_by:
                raise CompilationError(
                    f"{model.name}: flow='auto_cdc' requires `sequence_by` "
                    f"(e.g. sequence_by='updated_at')"
                )
            # FROM takes an identifier, not a subquery -> stage via temp view
            src_view = f"{model.dataset_name}__cdc"
            body = self._body(model, streaming=False)
            pre = f"CREATE TEMPORARY VIEW {src_view}\nAS\n{body};\n\n"
            flow.append(f"FROM STREAM({src_view})")
        flow.append(f"KEYS ({', '.join(cfg.keys)})")
        if cfg.ignore_null_updates:
            if cfg.ignore_null_updates is True:
                flow.append("IGNORE NULL UPDATES")
            else:
                v = cfg.ignore_null_updates
                if isinstance(v, dict) and "except" in v:
                    flow.append(f"IGNORE NULL UPDATES ON * EXCEPT ({', '.join(v['except'])})")
                else:
                    cols = v if isinstance(v, (list, tuple)) else [v]
                    flow.append(f"IGNORE NULL UPDATES ON {', '.join(cols)}")
        if cfg.apply_as_deletes:
            flow.append(f"APPLY AS DELETE WHEN {cfg.apply_as_deletes}")
        if cfg.apply_as_truncates:
            flow.append(f"APPLY AS TRUNCATE WHEN {cfg.apply_as_truncates}")
        if not snapshot:
            seq = cfg.sequence_by
            seq_sql = f"STRUCT({', '.join(seq)})" if isinstance(seq, (list, tuple)) else str(seq)
            flow.append(f"SEQUENCE BY {seq_sql}")
            cols = _colspec(cfg.columns, "COLUMNS")
            if cols:
                flow.append(cols)
        if cfg.stored_as:
            sa = STORED_AS.get(str(cfg.stored_as).lower())
            if not sa:
                raise CompilationError(
                    f"{model.name}: unknown stored_as {cfg.stored_as!r}; "
                    f"expected one of {sorted(STORED_AS)}"
                )
            flow.append(f"STORED AS {sa}")
        th = _colspec(cfg.track_history, "TRACK HISTORY ON")
        if th:
            flow.append(th)
        if cfg.into:
            # flow-only AUTO CDC: target table is created by another model
            self._warn_dropped_tests(model)
            return header + pre + "\n".join(flow) + ";\n"
        create = f"CREATE OR REFRESH {priv}STREAMING TABLE {qname}{spec}{clauses};"
        return header + pre + create + "\n\n" + "\n".join(flow) + ";\n"

    # -- public API ----------------------------------------------------------

    def resolve_datasets(self, selector: str,
                         within: Optional[Set[str]] = None) -> List[str]:
        """Selector(s) -> dataset names, for --refresh/--full-refresh flags.

        Accepts comma- or space-separated dbt selectors (`a`, `+a`, `a+`,
        `tag:t`, `resource_type:seed`, `path:d`, `source:s`). Ephemeral and
        flow-only (`into:`) nodes have no dataset and are dropped. ``within``
        restricts to a pipeline's emitted set.
        """
        names: List[str] = []
        for tok in re.split(r"[,\s]+", selector.strip()):
            if not tok:
                continue
            for n in self.graph.select(tok):
                if within is not None and n not in within:
                    continue
                m = self.parsed.models[n]
                if not m.is_ephemeral and not m.config.into:
                    names.append(m.qualified_name)
        deduped = list(dict.fromkeys(names))
        if not deduped and within is None:
            raise CompilationError(f"selector {selector!r} matched no datasets")
        return deduped

    # -- multi-pipeline compilation ------------------------------------------

    def pipeline_assignments(self) -> "Dict[str, Set[str]]":
        """pipeline name -> model names it owns (empty when `pipelines:` unset).

        Raises on overlap: a non-ephemeral dataset may be created by exactly
        one pipeline, or two `spark-pipelines` deployments will fight over
        the same table.
        """
        assigned: Dict[str, Set[str]] = {}
        owner: Dict[str, str] = {}
        for pname, spec in self.project.pipelines.items():
            sel = set(self.graph.select(spec["select"]))
            for n in sorted(sel):
                m = self.parsed.models[n]
                if m.is_ephemeral:
                    continue
                if n in owner:
                    raise CompilationError(
                        f"model {n!r} is selected by pipelines {owner[n]!r} "
                        f"and {pname!r} — a dataset can be owned by only one "
                        f"pipeline"
                    )
                owner[n] = pname
            assigned[pname] = sel
        # coverage: a dataset upstream of a pipeline member but in no
        # pipeline will never be created — flag it
        covered = set(owner)
        for pname, sel in assigned.items():
            for n in sorted(sel):
                for a in self.graph.ancestors(n):
                    am = self.parsed.models[a]
                    if not am.is_ephemeral and a not in covered:
                        self.warnings.append(
                            f"pipeline {pname!r}: {n!r} refs {a!r}, which is "
                            f"assigned to no pipeline — it resolves to "
                            f"{am.qualified_name} as an external read"
                        )
        return assigned

    def compile_pipelines(
        self, select: Optional[str] = None,
        pipeline: Optional[List[str]] = None,
        target_dir: Optional[Path] = None,
    ) -> "Dict[str, CompileResult]":
        """Compile every pipeline (or the named subset) to its own spec dir.

        With no `pipelines:` config this is the single-pipeline path: one
        spec at ``target_dir`` containing the --select closure. With
        `pipelines:` each entry emits ``target_dir/pipelines/<name>/``.
        """
        root_target = Path(target_dir or (self.project.root / self.project.target_path))
        if not self.project.pipelines:
            return {self.project.name: self._compile_one(
                self.graph.topo_order(self.graph.select(select)),
                root_target, self.project.name)}

        assigned = self.pipeline_assignments()
        results: Dict[str, CompileResult] = {}
        for pname, sel in assigned.items():
            if pipeline and pname not in pipeline:
                continue
            if select:
                # CLI narrowing: keep the selected slice of this pipeline
                sel &= set(self.graph.topo_order(self.graph.select(select)))
            order = [n for n in self.graph.topo_order() if n in sel]
            results[pname] = self._compile_one(
                order, root_target / "pipelines" / pname, pname,
                spec_overrides=self.project.pipelines[pname])
        if not results:
            raise CompilationError(
                f"no pipelines matched {pipeline!r}; defined: "
                f"{', '.join(self.project.pipelines)}"
            )
        return results

    def _compile_one(self, order: List[str], target: Path,
                     spec_name: str,
                     spec_overrides: Optional[Dict] = None) -> CompileResult:
        """Emit one pipeline's definitions + spec + manifest for ``order``."""
        self._emitted = set(order)
        self._stream_views = set()
        for n in order:
            if self.parsed.models[n].materialization == "streaming_table":
                for a in self.graph.ancestors(n) & self._emitted:
                    am = self.parsed.models[a]
                    if am.materialization in ("view", "temporary_view"):
                        if am.materialization == "view":
                            self.warnings.append(
                                f"{a}: 'view' feeds a streaming table; emitted as "
                                f"TEMPORARY VIEW (published views can't be streaming sources)"
                            )
                        self._stream_views.add(a)
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
            ext = ".py" if model.language == "python" else ".sql"
            f = defs / f"{model.dataset_name}{ext}"
            f.write_text(sql)
            files.append(f)
            nodes[name] = _manifest_node(model, emitted=str(f.relative_to(target)))

        spec = self._write_spec(target, spec_name, spec_overrides)
        manifest = self._write_manifest(target, nodes)
        return CompileResult(target_dir=target, spec_file=spec,
                             manifest_file=manifest, files=files,
                             warnings=self.warnings)

    def compile(self, select: Optional[str] = None,
                target_dir: Optional[Path] = None) -> CompileResult:
        """Compile everything and merge into one result (back-compat API)."""
        results = self.compile_pipelines(select=select, target_dir=target_dir)
        merged_files: List[Path] = []
        for r in results.values():
            merged_files.extend(r.files)
        root_target = Path(target_dir or (self.project.root / self.project.target_path))
        first = next(iter(results.values()))
        self.warnings = sorted(set(self.warnings))
        return CompileResult(target_dir=first.target_dir if len(results) == 1 else root_target,
                             spec_file=first.spec_file,
                             manifest_file=first.manifest_file,
                             files=merged_files,
                             warnings=self.warnings)

    def _write_spec(self, target: Path, name: str,
                    overrides: Optional[Dict] = None) -> Path:
        ov = overrides or {}
        spec: Dict = {
            "name": name,
            "libraries": [{"glob": {"include": "definitions/**"}}],
        }
        storage = (ov.get("storage") or self.project.storage
                   or f"file://{(target / 'storage').resolve()}")
        spec["storage"] = storage
        catalog = ov.get("catalog") or self.project.catalog
        if catalog:
            spec["catalog"] = catalog
        schema = ov.get("schema") or ov.get("database") or self.project.schema
        if schema:
            spec["database"] = schema
        conf = {**self.project.configuration, **(ov.get("configuration") or {})}
        if conf:
            spec["configuration"] = conf
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
        "resource_type": model.resource_type,
        "dataset": model.qualified_name,
        "path": str(model.rel_path),
        "materialization": model.materialization,
        "refs": list(model.refs),
        "sources": list(model.sources),
        "emitted": emitted,
        "tags": model.config.tags,
        "language": model.language,
        "checksum": model.checksum,
        "columns": model.columns,
        "description": model.description,
    }
