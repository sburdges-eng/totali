import json
import subprocess
import sys
from pathlib import Path


def _is_absolute_path(value: str) -> bool:
    return value.startswith("/") or (len(value) >= 3 and value[1] == ":" and value[2] in {"/", "\\"})


def _write_json(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_eval_gate_script_passes_for_healthy_metrics(repo_root: Path, tmp_path: Path) -> None:
    metrics = tmp_path / "metrics.json"
    baseline = tmp_path / "baseline.json"
    _write_json(
        metrics,
        {
            "quality": {"heldout_score": 0.9, "failure_buckets": {"hard_case": 0.86}},
            "stability": {},
            "cost": {"cost_per_run_usd": 20.0},
            "latency": {"p95_ms": 200.0},
            "curation": {"hard_negative_share": 0.2},
        },
    )
    _write_json(
        baseline,
        {
            "quality": {"heldout_score": 0.91},
        },
    )

    run = subprocess.run(
        [
            sys.executable,
            str(repo_root / "scripts/eval_gate.py"),
            "--metrics",
            str(metrics),
            "--baseline",
            str(baseline),
        ],
        cwd=repo_root,
        capture_output=True,
        text=True,
        check=False,
    )
    assert run.returncode == 0, run.stdout + run.stderr

    payload = json.loads(run.stdout)
    assert list(payload.keys()) == [
        "schemaVersion",
        "artifactType",
        "invariants",
        "metadata",
        "paths",
        "data",
    ]
    assert payload["artifactType"] == "eval_gate_report"
    assert payload["metadata"]["ok"] is True
    assert payload["data"]["evaluation"]["gates"]["quality"]["ok"] is True
    for value in payload["paths"].values():
        if value is None:
            continue
        assert not _is_absolute_path(value)


def test_eval_gate_script_fails_for_quality_regression(repo_root: Path, tmp_path: Path) -> None:
    metrics = tmp_path / "metrics.json"
    baseline = tmp_path / "baseline.json"
    _write_json(
        metrics,
        {
            "quality": {"heldout_score": 0.7, "failure_buckets": {"hard_case": 0.6}},
            "stability": {},
            "cost": {"cost_per_run_usd": 20.0},
            "latency": {"p95_ms": 200.0},
            "curation": {"hard_negative_share": 0.2},
        },
    )
    _write_json(
        baseline,
        {
            "quality": {"heldout_score": 0.9},
        },
    )

    run = subprocess.run(
        [
            sys.executable,
            str(repo_root / "scripts/eval_gate.py"),
            "--metrics",
            str(metrics),
            "--baseline",
            str(baseline),
        ],
        cwd=repo_root,
        capture_output=True,
        text=True,
        check=False,
    )
    assert run.returncode == 1, run.stdout + run.stderr

    payload = json.loads(run.stdout)
    assert payload["metadata"]["ok"] is False
    assert payload["data"]["evaluation"]["gates"]["quality"]["ok"] is False
