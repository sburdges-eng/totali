"""Tests for :mod:`totali.external.demo`.

This reference CLI wires the Phase-1/2/3 external-adapter stack together
with the prompt-builder sanitizer. The tests here pin:

* the demo runs on the committed default fixture,
* the return shape is the documented dict (keys + coarse value types),
* the demo is deterministic: two invocations on the same fixture yield
  identical summary dicts,
* the CLI main exits 0 on the default happy path.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

# ``DxfAuracadAdapter`` routes through ``dwg-tool-parser/scripts/parse_dxf.py``
# which imports ezdxf, and the schema round-trip the demo depends on needs
# jsonschema at import-time in sibling tests. Mirror the sibling guard
# pattern so this test module opts out cleanly on minimal installs.
pytest.importorskip("ezdxf")
pytest.importorskip("jsonschema")

from totali.external.demo import demo

REPO_ROOT = Path(__file__).resolve().parents[1]
FIXTURE_DXF = REPO_ROOT / "dwg-tool-parser" / "fixtures" / "sample.dxf"


# ===================================================================
# 1. demo() on the default fixture
# ===================================================================


def test_demo_runs_on_default_fixture() -> None:
    """``demo()`` with no args runs against the committed fixture.

    Asserts the documented dict shape + coarse typing. Not pinning the
    exact entity count because the fixture is shared with other test
    modules that may regenerate it; the dict-shape contract is what the
    demo owes callers.
    """

    assert FIXTURE_DXF.exists(), (
        f"committed DXF fixture missing at {FIXTURE_DXF}; "
        "regenerate via tests/test_dwg_parser_ezdxf.py first"
    )

    summary = demo()

    expected_keys = {
        "report_file",
        "entity_count",
        "anomaly_scores",
        "proposal_count",
        "capsule_warning_count",
    }
    assert set(summary.keys()) == expected_keys

    assert isinstance(summary["report_file"], str)
    assert summary["report_file"].endswith("sample.dxf")

    assert isinstance(summary["entity_count"], int)
    assert summary["entity_count"] >= 1

    assert isinstance(summary["anomaly_scores"], list)
    assert len(summary["anomaly_scores"]) == summary["entity_count"]
    for score in summary["anomaly_scores"]:
        assert isinstance(score, float)
        assert 0.0 <= score < 1.0

    assert isinstance(summary["proposal_count"], int)
    # top_k=3 is the cap the demo passes; the stub never returns more.
    assert 0 <= summary["proposal_count"] <= 3

    # The demo's hardcoded cad_summary block is clean by construction.
    assert summary["capsule_warning_count"] == 0


# ===================================================================
# 2. Determinism across runs
# ===================================================================


def test_demo_returns_deterministic_output_across_runs() -> None:
    """Two successive ``demo()`` calls on the same fixture agree bitwise.

    The adapters + sanitizer are pure-functional at the inputs the demo
    uses. This test pins that surface so a future refactor that sneaks
    in a wall-clock, unseeded RNG, or env-dependent field fails here.
    """

    first = demo(dxf_path=FIXTURE_DXF)
    second = demo(dxf_path=FIXTURE_DXF)
    assert first == second


# ===================================================================
# 3. main() exit code on default path
# ===================================================================


def test_main_exits_zero_on_default() -> None:
    """Subprocess invocation of the CLI with no args exits 0."""

    result = subprocess.run(
        [sys.executable, "-m", "totali.external.demo"],
        cwd=str(REPO_ROOT),
        capture_output=True,
        text=True,
        timeout=60,
    )

    # Surface stderr on failure so regressions are easy to debug.
    assert result.returncode == 0, (
        f"demo CLI exited {result.returncode}; stderr:\n{result.stderr}"
    )
    # Sanity-check that the summary header actually made it to stdout —
    # a 0 exit on a silent script would miss an upstream import error.
    assert "external-stack demo" in result.stdout
