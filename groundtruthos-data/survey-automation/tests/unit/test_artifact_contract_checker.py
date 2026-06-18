import json
import subprocess
import sys
from copy import deepcopy
from pathlib import Path

from survey_automation.bridge import run_bridge
from survey_automation.config import DEFAULT_CONFIG
from survey_automation.pipeline import run_pipeline


def _write_point_csv(path: Path) -> None:
    path.write_text(
        "Point#,Northing,Easting,Elevation,Description,DWG Description,DWG Layer,Locked,Group,Category,LS Number\n"
        "1,1000,2000,300,CP TEST,CP TEST,PNTS,No,,Control,\n",
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


def test_artifact_contract_checker_passes_for_canonical_outputs(repo_root: Path, tmp_path: Path) -> None:
    input_dir = tmp_path / "input"
    input_dir.mkdir(parents=True, exist_ok=True)
    _write_point_csv(input_dir / "points.csv")

    run_pipeline(
        input_dir=input_dir,
        output_dir=tmp_path / "artifacts",
        config=_base_config(),
        run_id="contract-check",
    )
    run_bridge(
        run_root=tmp_path / "artifacts/contract-check",
        rules_path=repo_root / "config/bridge_rules.example.yaml",
    )

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
    baseline.write_text(
        json.dumps({"quality": {"heldout_score": 0.91}}),
        encoding="utf-8",
    )
    eval_report = tmp_path / "artifacts/eval_gate_report.json"
    eval_run = subprocess.run(
        [
            sys.executable,
            str(repo_root / "scripts/eval_gate.py"),
            "--metrics",
            str(metrics),
            "--baseline",
            str(baseline),
            "--output",
            str(eval_report),
        ],
        cwd=repo_root,
        capture_output=True,
        text=True,
        check=False,
    )
    assert eval_run.returncode == 0, eval_run.stdout + eval_run.stderr

    check_run = subprocess.run(
        [
            sys.executable,
            str(repo_root / "scripts/check_artifact_contracts.py"),
            "--run-root",
            str(tmp_path / "artifacts/contract-check"),
            "--eval-report",
            str(eval_report),
        ],
        cwd=repo_root,
        capture_output=True,
        text=True,
        check=False,
    )
    assert check_run.returncode == 0, check_run.stdout + check_run.stderr


def test_artifact_contract_checker_fails_on_absolute_paths(repo_root: Path, tmp_path: Path) -> None:
    input_dir = tmp_path / "input"
    input_dir.mkdir(parents=True, exist_ok=True)
    _write_point_csv(input_dir / "points.csv")

    run_pipeline(
        input_dir=input_dir,
        output_dir=tmp_path / "artifacts",
        config=_base_config(),
        run_id="contract-check-fail",
    )
    run_root = tmp_path / "artifacts/contract-check-fail"
    run_bridge(
        run_root=run_root,
        rules_path=repo_root / "config/bridge_rules.example.yaml",
    )

    run_manifest_path = run_root / "manifest/run_manifest.json"
    run_manifest = json.loads(run_manifest_path.read_text(encoding="utf-8"))
    run_manifest["paths"]["run_root"] = "/absolute/path"
    run_manifest_path.write_text(json.dumps(run_manifest, indent=2), encoding="utf-8")

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
    baseline.write_text(
        json.dumps({"quality": {"heldout_score": 0.91}}),
        encoding="utf-8",
    )
    eval_report_path = tmp_path / "artifacts/eval_gate_report.json"
    eval_run = subprocess.run(
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
    assert eval_run.returncode == 0, eval_run.stdout + eval_run.stderr

    check_run = subprocess.run(
        [
            sys.executable,
            str(repo_root / "scripts/check_artifact_contracts.py"),
            "--run-root",
            str(run_root),
            "--eval-report",
            str(eval_report_path),
        ],
        cwd=repo_root,
        capture_output=True,
        text=True,
        check=False,
    )
    assert check_run.returncode == 1
