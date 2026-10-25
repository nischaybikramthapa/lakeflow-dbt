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
    result = Compiler(project, parsed).compile(select=args.select)
    _print_warnings(result.warnings)
    print(f"Compiled {len(result.files)} definition file(s) -> {result.target_dir}")
    for f in result.files:
        print(f"  {f.relative_to(result.target_dir)}")
    print(f"  {result.spec_file.relative_to(result.target_dir)}")
    return 0


def cmd_ls(args) -> int:
    project, parsed = _load(args)
    compiler = Compiler(project, parsed)
    for name in compiler.graph.select(args.select):
        m = parsed.models[name]
        print(f"{name:30s} {m.materialization:18s} {m.rel_path}")
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


def _refresh_cmd(args, compiler: Compiler) -> list:
    """Translate --refresh/--full-refresh selectors into spark-pipelines args."""
    if args.full_refresh_all and (args.refresh or args.full_refresh):
        raise LakeflowDbtError(
            "--full-refresh-all can't be combined with --refresh/--full-refresh"
        )
    out: list = []
    if args.refresh:
        ds = compiler.resolve_datasets(args.refresh)
        out += ["--refresh", ",".join(ds)]
    if args.full_refresh:
        ds = compiler.resolve_datasets(args.full_refresh)
        out += ["--full-refresh", ",".join(ds)]
    if args.full_refresh_all:
        out += ["--full-refresh-all"]
    return out


def cmd_run(args) -> int:
    project, parsed = _load(args)
    compiler = Compiler(project, parsed)
    # `lfd refresh --select x` == `lfd run --refresh x`
    if getattr(args, "refresh_selector", False) and not (args.refresh or args.full_refresh):
        args.refresh = args.select
    # when refresh selectors are given the spec must contain every dataset:
    # compile the whole project, not a --select-trimmed subset
    has_refresh = args.refresh or args.full_refresh or args.full_refresh_all
    select = None if has_refresh else args.select
    result = compiler.compile(select=select)
    _print_warnings(result.warnings)

    tool = "dry-run" if args.dry_run else "run"
    cmd = ["spark-pipelines", tool, "--spec", str(result.spec_file)]
    cmd += _refresh_cmd(args, compiler)
    cmd += args.spark_args or []

    if shutil.which("spark-pipelines") is None:
        print("`spark-pipelines` CLI not found on PATH.")
        print("Install it with `pip install 'pyspark[pipelines]'`, or on Databricks")
        print("deploy target/ via a bundle pointing at the generated spec, then run:")
        print("  " + " ".join(cmd))
        return 2
    print("+ " + " ".join(cmd))
    return subprocess.call(cmd, cwd=result.target_dir)


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
        s.add_argument("--select", help="dbt-style selection: `+a b+ tag:x`")
        s.add_argument("--vars", help="inline JSON/YAML map of vars for {{ var() }}")
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
    return p


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return args.fn(args)
    except LakeflowDbtError as e:
        print(f"error: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
