"""Tests for :mod:`totali.sales_demo` (M1 seven-step Key Flow)."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

pytest.importorskip("laspy")
pytest.importorskip("pyproj")

REPO_ROOT = Path(__file__).resolve().parents[1]
FIXTURE_LAS = REPO_ROOT / "tests" / "fixtures" / "survey_corpus" / "synth_topo.las"


def test_fixture_las_exists() -> None:
    assert FIXTURE_LAS.is_file(), f"missing {FIXTURE_LAS}; run tools/generate_synth_topo_las.py"


def test_sales_demo_subprocess_exits_zero() -> None:
    """Full demo via subprocess (avoids pytest laspy stub in conftest)."""
    env = {k: v for k, v in os.environ.items() if k != "TOTALI_PARTNER_LAS"}
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "totali.sales_demo",
            "--input",
            str(FIXTURE_LAS),
            "--project-id",
            "SALES-DEMO-TEST",
        ],
        cwd=str(REPO_ROOT),
        env=env,
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert result.returncode == 0, (
        f"sales_demo exited {result.returncode}; stderr:\n{result.stderr}"
    )
    assert "TOTaLi sales demo" in result.stdout
    assert "audit_verify_ok: True" in result.stdout
    assert "classification_authoritative: False" in result.stdout
