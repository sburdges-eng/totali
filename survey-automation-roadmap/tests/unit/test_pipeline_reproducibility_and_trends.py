import json
from copy import deepcopy
from pathlib import Path

from survey_automation.config import DEFAULT_CONFIG
from survey_automation.pipeline import run_pipeline


def _is_absolute_path(value: str) -> bool:
    return value.startswith("/") or (len(value) >= 3 and value[1] == ":" and value[2] in {"/", "\\"})


def _assert_contract_key_order(payload: dict) -> None:
    assert list(payload.keys()) == [
        "schemaVersion",
        "artifactType",
        "invariants",
        "metadata",
        "paths",
        "data",
    ]


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
    config["validation"]["trend_tracking"]["enabled"] = True
    config["validation"]["trend_tracking"]["fail_on_spike"] = True
    config["validation"]["trend_tracking"]["max_warning_delta"] = 0
    config["validation"]["trend_tracking"]["max_error_delta"] = 0
    config["validation"]["trend_tracking"]["max_critical_delta"] = 0
    return config


def test_run_writes_dataset_snapshot_with_checksums(tmp_path: Path) -> None:
    input_dir = tmp_path / "input"
    input_dir.mkdir(parents=True, exist_ok=True)
    _write_point_csv(input_dir / "points.csv")

    result = run_pipeline(
        input_dir=input_dir,
        output_dir=tmp_path / "out",
        config=_base_config(),
        run_id="snapshot-run",
    )
    assert result.exit_code in {0, 2}

    snapshot_path = tmp_path / "out/snapshot-run/manifest/dataset_snapshot.json"
    snapshot = json.loads(snapshot_path.read_text(encoding="utf-8"))
    _assert_contract_key_order(snapshot)
    assert snapshot["artifactType"] == "dataset_snapshot"
    assert snapshot["data"]["snapshot_id"].startswith("sha256:")
    assert snapshot["data"]["file_count"] == 1
    assert snapshot["data"]["files"][0]["relative_path"] == "points.csv"
    assert len(snapshot["data"]["files"][0]["sha256"]) == 64
    for value in snapshot["paths"].values():
        assert not _is_absolute_path(value)

    run_manifest = json.loads((tmp_path / "out/snapshot-run/manifest/run_manifest.json").read_text(encoding="utf-8"))
    _assert_contract_key_order(run_manifest)
    assert run_manifest["artifactType"] == "run_manifest"
    assert run_manifest["data"]["dataset_snapshot"]["snapshot_id"] == snapshot["data"]["snapshot_id"]
    assert (
        run_manifest["data"]["phase_presentation"]["ground_truth"]["evidence"]["snapshot_id"]
        == snapshot["data"]["snapshot_id"]
    )
    assert run_manifest["data"]["remediation_row_counts"]["invariant_satisfied"] is True
    for value in run_manifest["paths"].values():
        assert not _is_absolute_path(value)


def test_qc_trend_spike_fails_run_on_warning_regression(tmp_path: Path) -> None:
    input_dir = tmp_path / "input"
    input_dir.mkdir(parents=True, exist_ok=True)
    _write_point_csv(input_dir / "points.csv")
    (input_dir / "sample.crd").write_bytes(b"\x00\x00New CRD Format2\x00\x00")
    output_dir = tmp_path / "out"

    config = _base_config()
    baseline = run_pipeline(
        input_dir=input_dir,
        output_dir=output_dir,
        config=config,
        run_id="trend-baseline",
    )
    assert baseline.exit_code == 2

    (input_dir / "binary.dxf").write_bytes(b"AutoCAD Binary DXF\x00\x01")

    regressed = run_pipeline(
        input_dir=input_dir,
        output_dir=output_dir,
        config=config,
        run_id="trend-regressed",
    )
    assert regressed.exit_code == 3
    assert any(finding.code == "qc_regression_spike" for finding in regressed.findings)

    trend_payload = json.loads((output_dir / "trend-regressed/reports/qc_trend.json").read_text(encoding="utf-8"))
    _assert_contract_key_order(trend_payload)
    assert trend_payload["artifactType"] == "qc_trend"
    assert trend_payload["data"]["comparison_available"] is True
    assert trend_payload["data"]["spike_detected"] is True
    assert any(item["severity"] == "warning" for item in trend_payload["data"]["spikes"])


def test_qc_trend_baselines_are_namespaced_by_project(tmp_path: Path) -> None:
    project_a = tmp_path / "project-a"
    project_b = tmp_path / "project-b"
    project_a.mkdir(parents=True, exist_ok=True)
    project_b.mkdir(parents=True, exist_ok=True)

    _write_point_csv(project_a / "points.csv")
    _write_point_csv(project_b / "points.csv")
    (project_a / "sample.crd").write_bytes(b"\x00\x00New CRD Format2\x00\x00")
    (project_b / "sample.crd").write_bytes(b"\x00\x00New CRD Format2\x00\x00")

    output_dir = tmp_path / "out"
    config = _base_config()

    baseline_a = run_pipeline(
        input_dir=project_a,
        output_dir=output_dir,
        config=config,
        run_id="baseline-a",
    )
    assert baseline_a.exit_code == 2

    # Add an extra warning only in project B; this should not compare against project A baseline.
    (project_b / "binary.dxf").write_bytes(b"AutoCAD Binary DXF\x00\x01")
    run_b = run_pipeline(
        input_dir=project_b,
        output_dir=output_dir,
        config=config,
        run_id="run-b",
    )
    assert run_b.exit_code == 2

    trend_payload = json.loads((output_dir / "run-b/reports/qc_trend.json").read_text(encoding="utf-8"))
    assert trend_payload["data"]["comparison_available"] is False
    assert trend_payload["data"]["spike_detected"] is False


def test_trend_state_resolution_is_independent_of_working_directory(tmp_path: Path, monkeypatch) -> None:
    input_dir = tmp_path / "input"
    input_dir.mkdir(parents=True, exist_ok=True)
    _write_point_csv(input_dir / "points.csv")
    (input_dir / "sample.crd").write_bytes(b"\x00\x00New CRD Format2\x00\x00")

    config_dir = tmp_path / "config"
    config_dir.mkdir(parents=True, exist_ok=True)
    output_dir = tmp_path / "out"
    work_a = tmp_path / "work-a"
    work_b = tmp_path / "work-b"
    work_a.mkdir(parents=True, exist_ok=True)
    work_b.mkdir(parents=True, exist_ok=True)

    config = _base_config()
    config["project"]["baseline_namespace"] = "cwd-stable"
    config["validation"]["trend_tracking"]["state_file_path"] = "trend_state/{namespace}/last_good_run.json"

    monkeypatch.chdir(work_a)
    baseline = run_pipeline(
        input_dir=input_dir,
        output_dir=output_dir,
        config=config,
        run_id="cwd-baseline",
        config_anchor_dir=config_dir,
    )
    assert baseline.exit_code == 2

    (input_dir / "binary.dxf").write_bytes(b"AutoCAD Binary DXF\x00\x01")

    monkeypatch.chdir(work_b)
    regressed = run_pipeline(
        input_dir=input_dir,
        output_dir=output_dir,
        config=config,
        run_id="cwd-regressed",
        config_anchor_dir=config_dir,
    )
    assert regressed.exit_code == 3

    trend_payload = json.loads((output_dir / "cwd-regressed/reports/qc_trend.json").read_text(encoding="utf-8"))
    assert trend_payload["data"]["comparison_available"] is True
    assert trend_payload["data"]["spike_detected"] is True

    state_path = config_dir / "trend_state/cwd-stable/last_good_run.json"
    assert state_path.exists()
