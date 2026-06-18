"""CLI exit-code semantics for ``totali.main``.

A gate that halts the pipeline (e.g. the geodetic gatekeeper rejecting an
input) returns ``PipelineResult.success == False`` with no outputs. The CLI
must surface that as a non-zero exit — reporting ``✓ Pipeline complete`` and
exit 0 on a failed gate would let callers/CI read an empty output set as OK.
"""

from pathlib import Path

import yaml
from click.testing import CliRunner

from totali.main import main

_REPO_ROOT = Path(__file__).resolve().parents[1]
_BASE_CONFIG = _REPO_ROOT / "config" / "pipeline.yaml"


def _tmp_config(tmp_path: Path) -> Path:
    """Base config with the audit log redirected under tmp_path (hermetic)."""
    cfg = yaml.safe_load(_BASE_CONFIG.read_text())
    cfg.setdefault("audit", {})["log_dir"] = str(tmp_path / "audit")
    p = tmp_path / "pipeline.yaml"
    p.write_text(yaml.safe_dump(cfg))
    return p


def _coded_survey(tmp_path: Path) -> Path:
    # A coded-survey (.txt) export. With no field-code library configured the
    # geodetic gate halts immediately — a deterministic failure with no ML deps.
    p = tmp_path / "job.txt"
    p.write_text("1,1358504.5,2822466.65,7943.9,7V1 B\n")
    return p


def test_failed_phase_exits_nonzero(tmp_path, monkeypatch):
    monkeypatch.delenv("TOTALI_FIELDCODE_FLD", raising=False)
    res = CliRunner().invoke(
        main,
        [
            "--input",
            str(_coded_survey(tmp_path)),
            "--config",
            str(_tmp_config(tmp_path)),
            "--output",
            str(tmp_path / "out"),
            "--project-id",
            "CLI_FAIL_TEST",
        ],
    )
    assert res.exit_code != 0, res.output
    assert "fail" in res.output.lower()


def test_dry_run_exits_zero(tmp_path, monkeypatch):
    monkeypatch.delenv("TOTALI_FIELDCODE_FLD", raising=False)
    res = CliRunner().invoke(
        main,
        [
            "--input",
            str(_coded_survey(tmp_path)),
            "--config",
            str(_tmp_config(tmp_path)),
            "--output",
            str(tmp_path / "out"),
            "--dry-run",
        ],
    )
    assert res.exit_code == 0, res.output
