import csv
import json
from copy import deepcopy

import pytest

from survey_automation.config import DEFAULT_CONFIG
from survey_automation.pipeline import run_pipeline


def _count_rows(path):
    with open(path, "r", encoding="utf-8", newline="") as handle:
        reader = csv.reader(handle)
        next(reader, None)
        return sum(1 for _ in reader)


def test_pipeline_run_on_totali_dataset(repo_root, tmp_path) -> None:
    totali_dir = repo_root / "TOTaLi"
    if not totali_dir.exists():
        pytest.skip("TOTaLi dataset is not present in this environment")

    config = deepcopy(DEFAULT_CONFIG)
    config["input"]["include_globs"] = ["TOTaLi/**/*"]
    config["input"]["exclude_globs"] = ["artifacts/**/*"]
    config["crd"]["mode"] = "auto"
    config["crd"]["converter_command"] = None

    result = run_pipeline(
        input_dir=repo_root,
        output_dir=tmp_path / "artifacts",
        config=config,
        run_id="totali-run",
    )

    assert result.exit_code == 2

    run_root = tmp_path / "artifacts" / "totali-run"
    assert (run_root / "normalized/points.csv").exists()
    assert (run_root / "normalized/points.parquet").exists()
    assert (run_root / "normalized/field_code_rules.csv").exists()
    assert (run_root / "normalized/dxf_entities.csv").exists()
    assert (run_root / "reports/qc_findings.jsonl").exists()
    assert (run_root / "reports/qc_summary.json").exists()
    assert (run_root / "quarantine/quarantined_rows.csv").exists()
    assert (run_root / "quarantine/quarantined_files.json").exists()
    assert (run_root / "manifest/run_manifest.json").exists()

    assert _count_rows(run_root / "normalized/points.csv") > 900
    assert _count_rows(run_root / "normalized/field_code_rules.csv") > 40
    assert _count_rows(run_root / "normalized/dxf_entities.csv") > 1000
    assert _count_rows(run_root / "quarantine/quarantined_rows.csv") >= 1

    quarantined_files = json.loads((run_root / "quarantine/quarantined_files.json").read_text(encoding="utf-8"))
    reasons = {entry["reason"] for entry in quarantined_files["files"]}
    assert "binary_crd_converter_missing" in reasons
    assert "unsupported_file_type" in reasons

    manifest = json.loads((run_root / "manifest/run_manifest.json").read_text(encoding="utf-8"))
    assert manifest["summary"]["files_total"] >= 10
    assert manifest["summary"]["files_processed"] >= 3
