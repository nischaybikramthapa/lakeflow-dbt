import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from lakeflow_dbt.parser import parse_project          # noqa: E402
from lakeflow_dbt.project import load_project         # noqa: E402

EXAMPLE = Path(__file__).resolve().parents[1] / "examples" / "jaffle_lake"


@pytest.fixture()
def project():
    return load_project(str(EXAMPLE))


@pytest.fixture()
def parsed(project):
    return parse_project(project)


@pytest.fixture()
def tmp_project(tmp_path):
    """Helper to build throwaway projects."""

    def make(files):
        for rel, content in files.items():
            p = tmp_path / rel
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_text(content)
        return tmp_path

    return make
