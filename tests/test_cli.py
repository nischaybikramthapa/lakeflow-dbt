import sys
from pathlib import Path

from lakeflow_dbt.cli import main

EXAMPLE = Path(__file__).resolve().parents[1] / "examples" / "jaffle_lake"


def test_parse_command(capsys):
    rc = main(["--project-dir", str(EXAMPLE), "parse"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "6 model(s)" in out
    assert "3 source table(s)" in out


def test_compile_command(capsys):
    rc = main(["--project-dir", str(EXAMPLE), "compile",
               "--vars", '{"currency": "EUR"}'])
    assert rc == 0
    out = capsys.readouterr().out
    assert "Compiled" in out
    target = EXAMPLE / "target"
    assert (target / "spark-pipeline.yml").exists()
    sql = (target / "definitions" / "customers.sql").read_text()
    assert "'EUR' as currency" in sql


def test_ls_command(capsys):
    rc = main(["--project-dir", str(EXAMPLE), "ls", "--select", "daily_revenue"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "daily_revenue" in out
    assert "stg_customers" not in out


def test_init_and_parse(tmp_path, capsys):
    assert main(["init", "newproj", str(tmp_path / "newproj")]) == 0
    capsys.readouterr()
    assert main(["--project-dir", str(tmp_path / "newproj"), "parse"]) == 0
    out = capsys.readouterr().out
    assert "1 model(s)" in out


def test_run_without_spark_pipelines(capsys, monkeypatch):
    monkeypatch.setenv("PATH", "/nonexistent")
    rc = main(["--project-dir", str(EXAMPLE), "run", "--dry-run"])
    out = capsys.readouterr().out
    assert rc == 2
    assert "spark-pipelines dry-run --spec" in out
