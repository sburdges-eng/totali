import csv
import json
from copy import deepcopy

from survey_automation.config import DEFAULT_CONFIG
from survey_automation.pipeline import run_pipeline


def _count_rows(path):
    with open(path, "r", encoding="utf-8", newline="") as handle:
        reader = csv.reader(handle)
        next(reader, None)
        return sum(1 for _ in reader)


def _file_text(path):
    return path.read_text(encoding="utf-8")


def _sample_config() -> dict:
    config = deepcopy(DEFAULT_CONFIG)
    config["input"]["include_globs"] = ["samples/input/**/*"]
    config["input"]["exclude_globs"] = ["artifacts/**/*"]
    config["crd"]["mode"] = "auto"
    config["crd"]["converter_command"] = None
    return config


def test_pipeline_smoke_on_samples(repo_root, tmp_path) -> None:
    result = run_pipeline(
        input_dir=repo_root,
        output_dir=tmp_path / "artifacts",
        config=_sample_config(),
        run_id="sample-run",
    )

    assert result.exit_code == 2

    run_root = tmp_path / "artifacts" / "sample-run"
    assert (run_root / "normalized/points.csv").exists()
    assert (run_root / "normalized/points.parquet").exists()
    assert (run_root / "normalized/field_code_rules.csv").exists()
    assert (run_root / "normalized/dxf_entities.csv").exists()
    assert (run_root / "reports/qc_findings.jsonl").exists()
    assert (run_root / "reports/qc_summary.json").exists()
    assert (run_root / "quarantine/quarantined_rows.csv").exists()
    assert (run_root / "quarantine/quarantined_files.json").exists()
    assert (run_root / "manifest/run_manifest.json").exists()

    assert _count_rows(run_root / "normalized/points.csv") >= 3
    assert _count_rows(run_root / "normalized/field_code_rules.csv") >= 3
    assert _count_rows(run_root / "normalized/dxf_entities.csv") >= 2

    manifest = json.loads((run_root / "manifest/run_manifest.json").read_text(encoding="utf-8"))
    summary = json.loads((run_root / "reports/qc_summary.json").read_text(encoding="utf-8"))
    assert manifest["metadata"]["tool_version"]
    assert "warning_threshold" in manifest["data"]
    assert "warning_threshold_exceeded" in manifest["data"]
    assert "phase_presentation" in summary["data"]
    assert "phase_presentation" in manifest["data"]
    assert summary["data"]["phase_presentation"]["ground_truth"]["evidence"]["snapshot_id"].startswith("sha256:")
    assert summary["data"]["phase_presentation"]["phase_1"]["status"] == "pass"
    assert manifest["data"]["phase_presentation"]["phase_3"]["evidence"]["exit_code"] == result.exit_code
    assert manifest["data"]["phase_presentation"]["phase_3"]["status"] == "warning"


def test_pipeline_is_deterministic_for_row_order_and_counts(repo_root, tmp_path) -> None:
    config = _sample_config()
    run1 = run_pipeline(
        input_dir=repo_root,
        output_dir=tmp_path / "artifacts",
        config=config,
        run_id="sample-run-1",
    )
    run2 = run_pipeline(
        input_dir=repo_root,
        output_dir=tmp_path / "artifacts",
        config=config,
        run_id="sample-run-2",
    )

    assert run1.exit_code == run2.exit_code
    assert run1.summary.findings_by_severity == run2.summary.findings_by_severity

    root1 = tmp_path / "artifacts" / "sample-run-1"
    root2 = tmp_path / "artifacts" / "sample-run-2"

    assert _file_text(root1 / "normalized/points.csv") == _file_text(root2 / "normalized/points.csv")
    assert _file_text(root1 / "normalized/field_code_rules.csv") == _file_text(
        root2 / "normalized/field_code_rules.csv"
    )
    assert _file_text(root1 / "normalized/dxf_entities.csv") == _file_text(root2 / "normalized/dxf_entities.csv")
    assert _file_text(root1 / "quarantine/quarantined_rows.csv") == _file_text(
        root2 / "quarantine/quarantined_rows.csv"
    )

    summary1 = json.loads((root1 / "reports/qc_summary.json").read_text(encoding="utf-8"))
    summary2 = json.loads((root2 / "reports/qc_summary.json").read_text(encoding="utf-8"))
    assert summary1["data"]["findings_by_severity"] == summary2["data"]["findings_by_severity"]
    assert summary1["data"]["point_count"] == summary2["data"]["point_count"]
    assert summary1["data"]["field_code_rule_count"] == summary2["data"]["field_code_rule_count"]
    assert summary1["data"]["dxf_entity_count"] == summary2["data"]["dxf_entity_count"]
    assert summary1["data"]["phase_presentation"] == summary2["data"]["phase_presentation"]
