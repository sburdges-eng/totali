import json
import subprocess
import sys
from copy import deepcopy
from pathlib import Path

from survey_automation.cli import main
from survey_automation.config import DEFAULT_CONFIG
from survey_automation.pipeline import run_pipeline


def _write_point_csv(path: Path, description: str) -> None:
    path.write_text(
        "Point#,Northing,Easting,Elevation,Description,DWG Description,DWG Layer,Locked,Group,Category,LS Number\n"
        f"1,1000,2000,300,{description},{description},PNTS,No,,Control,\n",
        encoding="utf-8",
    )


def _base_config() -> dict:
    config = deepcopy(DEFAULT_CONFIG)
    config["input"]["include_globs"] = ["**/*"]
    config["input"]["exclude_globs"] = []
    config["crd"]["mode"] = "auto"
    config["crd"]["converter_command"] = None
    config["validation"]["trend_tracking"]["enabled"] = False
    return config


def _write_eval_report(repo_root: Path, tmp_path: Path) -> Path:
    metrics = tmp_path / "metrics.json"
    baseline = tmp_path / "baseline.json"
    metrics.write_text(
        json.dumps(
            {
                "quality": {"heldout_score": 0.9, "failure_buckets": {"hard_case": 0.86}},
                "stability": {},
                "cost": {"cost_per_run_usd": 20.0},
                "latency": {"p95_ms": 200.0},
                "curation": {"hard_negative_share": 0.2},
            }
        ),
        encoding="utf-8",
    )
    baseline.write_text(json.dumps({"quality": {"heldout_score": 0.91}}), encoding="utf-8")

    eval_report_path = tmp_path / "eval_gate_report.json"
    run = subprocess.run(
        [
            sys.executable,
            str(repo_root / "scripts/eval_gate.py"),
            "--metrics",
            str(metrics),
            "--baseline",
            str(baseline),
            "--output",
            str(eval_report_path),
        ],
        cwd=repo_root,
        capture_output=True,
        text=True,
        check=False,
    )
    assert run.returncode == 0, run.stdout + run.stderr
    return eval_report_path


def test_arbitrate_command_passes_on_clean_bridge(repo_root: Path, tmp_path: Path, capsys) -> None:
    input_dir = tmp_path / "input"
    input_dir.mkdir(parents=True, exist_ok=True)
    _write_point_csv(input_dir / "points.csv", "CP TEST")

    run_pipeline(
        input_dir=input_dir,
        output_dir=tmp_path / "artifacts",
        config=_base_config(),
        run_id="arb-pass",
    )

    bridge_exit = main(
        [
            "bridge",
            "--run-root",
            str(tmp_path / "artifacts/arb-pass"),
            "--rules",
            str(repo_root / "config/bridge_rules.example.yaml"),
        ]
    )
    assert bridge_exit == 0
    capsys.readouterr()

    eval_report = _write_eval_report(repo_root, tmp_path)

    exit_code = main(
        [
            "arbitrate",
            "--run-root",
            str(tmp_path / "artifacts/arb-pass"),
            "--eval-report",
            str(eval_report),
        ]
    )
    assert exit_code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["ok"] is True
    assert payload["violation_count"] == 0


def test_arbitrate_command_fails_when_bridge_has_quarantine(repo_root: Path, tmp_path: Path, capsys) -> None:
    input_dir = tmp_path / "input"
    input_dir.mkdir(parents=True, exist_ok=True)
    _write_point_csv(input_dir / "points.csv", "UNKNOWN TOKEN")

    run_pipeline(
        input_dir=input_dir,
        output_dir=tmp_path / "artifacts",
        config=_base_config(),
        run_id="arb-fail",
    )

    bridge_exit = main(
        [
            "bridge",
            "--run-root",
            str(tmp_path / "artifacts/arb-fail"),
            "--rules",
            str(repo_root / "config/bridge_rules.example.yaml"),
        ]
    )
    assert bridge_exit == 0
    capsys.readouterr()

    eval_report = _write_eval_report(repo_root, tmp_path)

    exit_code = main(
        [
            "arbitrate",
            "--run-root",
            str(tmp_path / "artifacts/arb-fail"),
            "--eval-report",
            str(eval_report),
        ]
    )
    assert exit_code == 3
    payload = json.loads(capsys.readouterr().out)
    assert payload["ok"] is False
    assert payload["violation_count"] >= 1
