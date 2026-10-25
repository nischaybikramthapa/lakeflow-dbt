"""lakeflow-dbt / lfd command line."""
from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
from pathlib import Path

from .compiler import Compiler
from .errors import LakeflowDbtError
from .parser import parse_project
from .project import load_project

INIT_PROJECT = """\
name: {name}
version: '0.1'

# The pipeline these models compile into (spark-pipeline.yml fields)
catalog: main
schema: {name}
# storage: /Volumes/main/{name}/_checkpoints   # checkpoint dir for streaming tables

model-paths: [models]
target-path: target

# Per-directory defaults, dbt `+key` style:
models:
  {name}:
    staging:
      +materialized: view
    marts:
      +materialized: table
"""

INIT_SCHEMA = """\
version: 2

sources:
  - name: raw
    schema: raw_data            # reads resolve to <catalog>.raw_data.<table>
    tables:
      - name: example_events
        streaming: true         # read via STREAM() inside incremental models

models:
  - name: stg_example
    description: First staging model
    columns:
      - name: id
        tests: [not_null]
"""

INIT_MODEL = """\
{{ config(materialized='view') }}

select *
from {{ source('raw', 'example_events') }}
"""


def _parse_vars(raw: str):
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        import yaml

        try:
            v = yaml.safe_load(raw)
        except yaml.YAMLError as e:
            raise LakeflowDbtError(f"--vars must be JSON or YAML: {e}")
        if not isinstance(v, dict):
            raise LakeflowDbtError("--vars must be a mapping")
        return v


def _load(args):
    project = load_project(getattr(args, "project_dir", None),
                           vars_override=_parse_vars(getattr(args, "vars", "") or ""))
    parsed = parse_project(project)
    return project, parsed


def _state_map(project, args) -> dict:
    """name -> checksum from a previous manifest, for `state:` selectors.

    Reads --state <dir> or the project target dir (all manifests found).
    """
    from .docs import load_manifests
    state_dir = Path(getattr(args, "state", "") or
                     (project.root / project.target_path))
    if not state_dir.is_dir():
        return {}
    out = {}
    for m in load_manifests(state_dir).values():
        for name, n in (m.get("nodes") or {}).items():
            if n.get("checksum"):
                out[name] = n["checksum"]
    return out


def _compiler(project, parsed, args) -> Compiler:
    return Compiler(project, parsed, state=_state_map(project, args))


def _print_warnings(warnings, out=sys.stderr):
    for w in warnings:
        print(f"  warning: {w}", file=out)


def cmd_init(args) -> int:
    root = Path(args.directory or args.name).resolve()
    if root.exists() and any(root.iterdir()):
        raise LakeflowDbtError(f"{root} exists and is not empty")
    (root / "models" / "staging").mkdir(parents=True, exist_ok=True)
    (root / "macros").mkdir(exist_ok=True)
    (root / "lakeflow_project.yml").write_text(INIT_PROJECT.format(name=args.name))
    (root / "models" / "schema.yml").write_text(INIT_SCHEMA)
    (root / "models" / "staging" / "stg_example.sql").write_text(INIT_MODEL)
    print(f"Initialized lakeflow-dbt project in {root}")
    print("Next: edit lakeflow_project.yml, add models, then `lakeflow-dbt compile`")
    return 0


def cmd_parse(args) -> int:
    project, parsed = _load(args)
    compiler = Compiler(project, parsed)
    compiler.graph.topo_order()
    _print_warnings(parsed.warnings)
    print(f"{project.name}: {len(parsed.models)} model(s), "
          f"{len(parsed.sources)} source table(s) parsed OK")
    return 0


def cmd_compile(args) -> int:
    project, parsed = _load(args)
    results = _compiler(project, parsed, args).compile_pipelines(
        select=args.select, pipeline=args.pipeline)
    for pname, result in results.items():
        _print_warnings(result.warnings)
        print(f"pipeline {pname}: compiled {len(result.files)} "
              f"definition file(s) -> {result.target_dir}")
        for f in result.files:
            print(f"  {f.relative_to(result.target_dir)}")
        print(f"  {result.spec_file.relative_to(result.target_dir)}")
    return 0


def cmd_ls(args) -> int:
    project, parsed = _load(args)
    compiler = _compiler(project, parsed, args)
    assigned: dict = {}
    if project.pipelines:
        for pname, sel in compiler.pipeline_assignments().items():
            for n in sel:
                assigned[n] = pname
    for name in compiler.graph.select(args.select):
        m = parsed.models[name]
        pipe = f" [{assigned.get(name, '-')}]" if project.pipelines else ""
        print(f"{name:30s} {m.materialization:18s}{pipe} {m.rel_path}")
    return 0


def cmd_seed(args) -> int:
    """dbt `seed` analogue: compile only the seed CSVs."""
    project, parsed = _load(args)
    seeds = [n for n, m in parsed.models.items() if m.resource_type == "seed"]
    if not seeds:
        print("no seeds found under seed-paths")
        return 0
    result = Compiler(project, parsed).compile(select="resource_type:seed")
    _print_warnings(result.warnings)
    print(f"Compiled {len(result.files)} seed dataset(s) -> {result.target_dir}/definitions")
    for f in result.files:
        print(f"  {f.relative_to(result.target_dir)}")
    return 0


def _refresh_cmd(args, compiler: Compiler, within=None) -> list:
    """Translate --refresh/--full-refresh selectors into spark-pipelines args.

    ``within`` limits resolution to one pipeline's emitted datasets; a
    pipeline with no matching datasets gets no flags (nothing to refresh).
    """
    if args.full_refresh_all and (args.refresh or args.full_refresh):
        raise LakeflowDbtError(
            "--full-refresh-all can't be combined with --refresh/--full-refresh"
        )
    out: list = []
    if args.refresh:
        ds = compiler.resolve_datasets(args.refresh, within=within)
        if ds:
            out += ["--refresh", ",".join(ds)]
    if args.full_refresh:
        ds = compiler.resolve_datasets(args.full_refresh, within=within)
        if ds:
            out += ["--full-refresh", ",".join(ds)]
    if args.full_refresh_all:
        out += ["--full-refresh-all"]
    return out


def cmd_run(args) -> int:
    project, parsed = _load(args)
    compiler = _compiler(project, parsed, args)
    # `lfd refresh --select x` == `lfd run --refresh x`
    if getattr(args, "refresh_selector", False) and not (args.refresh or args.full_refresh):
        args.refresh = args.select
    # when refresh selectors are given the spec must contain every dataset:
    # compile the whole project, not a --select-trimmed subset
    has_refresh = args.refresh or args.full_refresh or args.full_refresh_all
    select = None if has_refresh else args.select
    results = compiler.compile_pipelines(select=select, pipeline=args.pipeline)
    for r in results.values():
        _print_warnings(r.warnings)

    tool = "dry-run" if args.dry_run else "run"
    missing_cli = shutil.which("spark-pipelines") is None
    if missing_cli:
        print("`spark-pipelines` CLI not found on PATH.")
        print("Install it with `pip install 'pyspark[pipelines]'`, or on Databricks")
        print("deploy target/ via a bundle pointing at the generated spec, then run:")
    rc = 2 if missing_cli else 0
    for pname, result in results.items():
        emitted = compiler.pipeline_assignments().get(pname) if project.pipelines else None
        cmd = ["spark-pipelines", tool, "--spec", str(result.spec_file)]
        cmd += _refresh_cmd(args, compiler, within=emitted)
        cmd += args.spark_args or []
        if missing_cli:
            print("  " + " ".join(cmd))
            continue
        print(f"+ [{pname}] " + " ".join(cmd))
        rc = subprocess.call(cmd, cwd=result.target_dir) or rc
    return rc


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="lakeflow-dbt",
        description="dbt-style modeling that compiles to Lakeflow Declarative Pipelines",
    )
    p.add_argument("--project-dir", help="project root (defaults to cwd or a parent)")
    sub = p.add_subparsers(dest="command", required=True)

    s = sub.add_parser("init", help="scaffold a new project")
    s.add_argument("name")
    s.add_argument("directory", nargs="?", help="target dir (default: ./<name>)")
    s.set_defaults(fn=cmd_init)

    s = sub.add_parser("parse", help="parse and validate the project")
    s.set_defaults(fn=cmd_parse)

    for name, help_ in (
        ("compile", "compile models to declarative pipeline SQL + spec"),
        ("run", "compile, then run the pipeline via `spark-pipelines`"),
        ("refresh", "compile, then selectively update datasets (--refresh)"),
        ("ls", "list models"),
    ):
        s = sub.add_parser(name, help=help_)
        s.add_argument("--select", help="dbt-style selection: `+a b+ tag:x path:dir source:s`")
        s.add_argument("--pipeline", action="append",
                       help="only this `pipelines:` entry (repeatable)")
        s.add_argument("--vars", help="inline JSON/YAML map of vars for {{ var() }}")
        s.add_argument("--state", help="dir containing a previous manifest for state: selectors")
        s.set_defaults(fn={"compile": cmd_compile, "run": cmd_run,
                           "refresh": cmd_run, "ls": cmd_ls}[name])
        if name == "refresh":
            s.set_defaults(refresh_selector=True)
        if name in ("run", "refresh"):
            s.add_argument("--dry-run", action="store_true",
                           help="run `spark-pipelines dry-run` instead")
            s.add_argument("--refresh",
                           help="selectors of datasets to update, e.g. `marts+ tag:x`")
            s.add_argument("--full-refresh",
                           help="selectors of datasets to reset and recompute")
            s.add_argument("--full-refresh-all", action="store_true")
            s.add_argument("spark_args", nargs=argparse.REMAINDER,
                           help="extra args passed to spark-pipelines (after --)")
    s = sub.add_parser("seed", help="compile seed CSVs under seed-paths")
    s.add_argument("--vars", help="inline JSON/YAML map")
    s.set_defaults(fn=cmd_seed)

    s = sub.add_parser("test", help="emit set-level data-quality check queries")
    s.add_argument("--select")
    s.add_argument("--vars")
    s.set_defaults(fn=cmd_test)

    s = sub.add_parser("docs", help="generate/serve the lineage site")
    ds = s.add_subparsers(dest="docs_command", required=True)
    d = ds.add_parser("generate", help="write target/docs/index.html")
    d.add_argument("--vars")
    d.set_defaults(fn=cmd_docs)
    d = ds.add_parser("serve", help="generate and serve the site locally")
    d.add_argument("--vars")
    d.add_argument("--port", type=int, default=8080)
    d.set_defaults(fn=cmd_docs)

    s = sub.add_parser("deploy", help="write a DAB databricks.yml from pipelines:")
    s.add_argument("--vars")
    s.add_argument("--print", action="store_true", help="print instead of writing")
    s.add_argument("--out", help="output file (default <project>/databricks.yml)")
    s.set_defaults(fn=cmd_deploy)
    return p


def cmd_test(args) -> int:
    """dbt `test` analogue: emit check SQL for set-level tests."""
    project, parsed = _load(args)
    from .testgen import generate
    target = project.root / project.target_path
    files = generate(project, parsed, args.select, target)
    if not files:
        print("no checkable tests declared")
        return 0
    print(f"Wrote {len(files)} check file(s) -> {target / 'tests'}")
    for f in files:
        print(f"  {f.relative_to(target)}")
    print("Run them against the deployed datasets, e.g.:")
    print("  spark-sql -f target/tests/<model>.sql")
    return 0


def cmd_docs(args) -> int:
    project, _ = _load(args)
    from .docs import generate
    target = project.root / project.target_path
    try:
        f = generate(target, target / "docs")
    except FileNotFoundError as e:
        print(f"error: {e}", file=sys.stderr)
        return 1
    print(f"wrote {f}")
    if args.docs_command == "serve":
        import http.server
        import functools
        handler = functools.partial(http.server.SimpleHTTPRequestHandler,
                                    directory=str(target / "docs"))
        print(f"serving on http://localhost:{args.port} (Ctrl-C to stop)")
        http.server.ThreadingHTTPServer(("127.0.0.1", args.port), handler).serve_forever()
    return 0


def cmd_deploy(args) -> int:
    """Generate a Databricks Asset Bundle from `pipelines:`."""
    project, parsed = _load(args)
    import yaml
    pipes = project.pipelines or {project.name: {"select": ""}}
    resources = {"pipelines": {}}
    for pname, spec in pipes.items():
        res_name = f"{project.name}_{pname}" if project.pipelines else pname
        root = "target/pipelines" if project.pipelines else "target"
        entry = {
            "name": res_name,
            "catalog": spec.get("catalog") or project.catalog,
            "schema": spec.get("schema") or spec.get("database") or project.schema,
            "serverless": True,
            "libraries": [{"glob": {"include": f"{root}/{pname}/definitions/**"
                                        if project.pipelines else
                                        "target/definitions/**"}}],
        }
        resources["pipelines"][res_name] = {k: v for k, v in entry.items()
                                            if v is not None}
    bundle = {"bundle": {"name": project.name}, "resources": resources}
    text = yaml.safe_dump(bundle, sort_keys=False)
    if args.print:
        print(text)
        return 0
    out = Path(args.out) if args.out else project.root / "databricks.yml"
    out.write_text(text)
    print(f"wrote {out} — deploy with `databricks bundle deploy`")
    return 0


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return args.fn(args)
    except LakeflowDbtError as e:
        print(f"error: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
