import csv
import json
from pathlib import Path

from survey_automation.cli import main
from survey_automation.json_contract import build_contract_payload, write_contract_json


def _write_points_csv(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "point_id",
        "northing",
        "easting",
        "elevation",
        "description",
        "dwg_description",
        "dwg_layer",
        "locked",
        "group_name",
        "category",
        "ls_number",
        "source_file",
        "source_line",
    ]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerow(
            {
                "point_id": "1",
                "northing": "1000",
                "easting": "2000",
                "elevation": "300",
                "description": "CP START",
                "dwg_description": "CP START",
                "dwg_layer": "PNTS",
                "locked": "No",
                "group_name": "",
                "category": "Control",
                "ls_number": "",
                "source_file": "/repo/samples/input/points.csv",
                "source_line": "2",
            }
        )


def _write_dataset_snapshot(path: Path) -> None:
    payload = build_contract_payload(
        artifact_type="dataset_snapshot",
        invariants=["paths_are_relative", "deterministic_key_order"],
        metadata={"input_root": "."},
        paths={"input_root": "."},
        data={
            "snapshot_id": "sha256:test-snapshot",
            "snapshot_algorithm": "sha256",
            "file_count": 1,
            "files": [],
        },
    )
    write_contract_json(path, payload)


def _write_run_manifest(path: Path) -> None:
    payload = build_contract_payload(
        artifact_type="run_manifest",
        invariants=["paths_are_relative", "deterministic_key_order"],
        metadata={"run_id": "cli-run"},
        paths={"run_root": "."},
        data={
            "input_files": [
                {"file_path": "samples/input/points.csv"},
            ]
        },
    )
    write_contract_json(path, payload)


def _write_rule_pack(path: Path) -> None:
    path.write_text(
        "schemaVersion: '2.0.0'\n"
        "metadata:\n"
        "  rulePackId: 'cli-pack'\n"
        "  version: '1.0.0'\n"
        "precedence:\n"
        "  - 'exact_code'\n"
        "rules:\n"
        "  exact_code:\n"
        "    - id: 'cp_exact'\n"
        "      match: 'CP'\n"
        "      feature_code: 'control_point'\n"
        "      feature_type: 'point'\n"
        "      group_by: 'per_point'\n",
        encoding="utf-8",
    )


def test_bridge_command_succeeds_and_prints_summary(tmp_path: Path, capsys) -> None:
    run_root = tmp_path / "run-cli"
    rules_path = tmp_path / "rules.yaml"

    _write_points_csv(run_root / "normalized/points.csv")
    _write_dataset_snapshot(run_root / "manifest/dataset_snapshot.json")
    _write_run_manifest(run_root / "manifest/run_manifest.json")
    _write_rule_pack(rules_path)

    exit_code = main(
        [
            "bridge",
            "--run-root",
            str(run_root),
            "--rules",
            str(rules_path),
        ]
    )
    assert exit_code == 0

    payload = json.loads(capsys.readouterr().out)
    assert payload["ok"] is True
    assert payload["counts"]["mapped_points"] == 1
    assert payload["counts"]["unmapped_points"] == 0

    for rel_path in payload["artifact_paths"].values():
        assert not rel_path.startswith("/")
        assert (run_root / rel_path).exists()


def test_bridge_command_fails_when_points_artifact_is_missing(tmp_path: Path, capsys) -> None:
    run_root = tmp_path / "run-cli-fail"
    rules_path = tmp_path / "rules.yaml"

    _write_dataset_snapshot(run_root / "manifest/dataset_snapshot.json")
    _write_run_manifest(run_root / "manifest/run_manifest.json")
    _write_rule_pack(rules_path)

    exit_code = main(
        [
            "bridge",
            "--run-root",
            str(run_root),
            "--rules",
            str(rules_path),
        ]
    )
    assert exit_code == 3
    assert "Missing normalized points artifact" in capsys.readouterr().err
