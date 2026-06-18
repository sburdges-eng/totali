import csv
import json
from pathlib import Path

from survey_automation.bridge import run_bridge
from survey_automation.json_contract import build_contract_payload, is_absolute_path_value, write_contract_json


def _assert_contract_key_order(payload: dict) -> None:
    assert list(payload.keys()) == [
        "schemaVersion",
        "artifactType",
        "invariants",
        "metadata",
        "paths",
        "data",
    ]


def _iter_path_values(node):
    if isinstance(node, str):
        yield node
        return
    if isinstance(node, dict):
        for value in node.values():
            yield from _iter_path_values(value)
        return
    if isinstance(node, list):
        for value in node:
            yield from _iter_path_values(value)


def _write_points_csv(path: Path, rows: list[dict[str, str]]) -> None:
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
        writer.writerows(rows)


def _write_dataset_snapshot(path: Path, snapshot_id: str) -> None:
    payload = build_contract_payload(
        artifact_type="dataset_snapshot",
        invariants=["paths_are_relative", "deterministic_key_order"],
        metadata={"input_root": "."},
        paths={"input_root": "."},
        data={
            "snapshot_id": snapshot_id,
            "snapshot_algorithm": "sha256",
            "file_count": 1,
            "files": [
                {
                    "relative_path": "points.csv",
                    "size_bytes": 1,
                    "sha256": "0" * 64,
                }
            ],
        },
    )
    write_contract_json(path, payload)


def _write_run_manifest(path: Path) -> None:
    payload = build_contract_payload(
        artifact_type="run_manifest",
        invariants=["paths_are_relative", "deterministic_key_order"],
        metadata={"run_id": "sample-run"},
        paths={"run_root": "."},
        data={
            "input_files": [
                {"file_path": "samples/input/points.csv"},
                {"file_path": "samples/input/mixed.csv"},
            ]
        },
    )
    write_contract_json(path, payload)


def _write_rule_pack(path: Path) -> None:
    path.write_text(
        "schemaVersion: '2.0.0'\n"
        "metadata:\n"
        "  rulePackId: 'test-pack'\n"
        "  version: '1.0.0'\n"
        "precedence:\n"
        "  - 'exact_code'\n"
        "  - 'exact_phrase'\n"
        "  - 'regex'\n"
        "  - 'prefix'\n"
        "rules:\n"
        "  exact_code:\n"
        "    - id: 'cp_exact'\n"
        "      match: 'CP'\n"
        "      feature_code: 'control_point'\n"
        "      feature_type: 'point'\n"
        "      group_by: 'per_point'\n"
        "  exact_phrase:\n"
        "    - id: 'top_shoulder_exact'\n"
        "      match: 'TOP SHOULDER'\n"
        "      feature_code: 'top_shoulder'\n"
        "      feature_type: 'line_string'\n"
        "      group_by: 'code_and_source'\n"
        "  regex:\n"
        "    - id: 'freb_regex'\n"
        "      pattern: '^FREB\\b'\n"
        "      feature_code: 'found_rebar'\n"
        "      feature_type: 'point'\n"
        "      group_by: 'per_point'\n"
        "  prefix:\n"
        "    - id: 'top_prefix'\n"
        "      match: 'TOP'\n"
        "      feature_code: 'top_generic'\n"
        "      feature_type: 'line_string'\n"
        "      group_by: 'code_and_source'\n",
        encoding="utf-8",
    )


def _write_ambiguous_rule_pack(path: Path) -> None:
    path.write_text(
        "schemaVersion: '2.0.0'\n"
        "metadata:\n"
        "  rulePackId: 'ambiguous-pack'\n"
        "  version: '1.0.0'\n"
        "precedence:\n"
        "  - 'exact_code'\n"
        "rules:\n"
        "  exact_code:\n"
        "    - id: 'cp_a'\n"
        "      match: 'CP'\n"
        "      feature_code: 'control_a'\n"
        "      feature_type: 'point'\n"
        "      group_by: 'per_point'\n"
        "    - id: 'cp_b'\n"
        "      match: 'CP'\n"
        "      feature_code: 'control_b'\n"
        "      feature_type: 'point'\n"
        "      group_by: 'per_point'\n",
        encoding="utf-8",
    )


def _write_line_requires_two_points_rule_pack(path: Path) -> None:
    path.write_text(
        "schemaVersion: '2.0.0'\n"
        "metadata:\n"
        "  rulePackId: 'line-pack'\n"
        "  version: '1.0.0'\n"
        "precedence:\n"
        "  - 'exact_code'\n"
        "rules:\n"
        "  exact_code:\n"
        "    - id: 'top_line'\n"
        "      match: 'TOP'\n"
        "      feature_code: 'top_line'\n"
        "      feature_type: 'line_string'\n"
        "      group_by: 'code_and_source'\n",
        encoding="utf-8",
    )


def test_bridge_writes_canonical_contract_artifacts(tmp_path: Path) -> None:
    run_root = tmp_path / "run-one"
    rules_path = tmp_path / "bridge-rules.yaml"
    _write_rule_pack(rules_path)

    _write_points_csv(
        run_root / "normalized/points.csv",
        [
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
            },
            {
                "point_id": "2",
                "northing": "1001",
                "easting": "2001",
                "elevation": "301",
                "description": "FREB CHECK",
                "dwg_description": "FREB CHECK",
                "dwg_layer": "PNTS",
                "locked": "No",
                "group_name": "",
                "category": "Found Rebar",
                "ls_number": "",
                "source_file": "/repo/samples/input/points.csv",
                "source_line": "3",
            },
            {
                "point_id": "3",
                "northing": "1002",
                "easting": "2002",
                "elevation": "302",
                "description": "TOP SHOULDER",
                "dwg_description": "TOP SHOULDER",
                "dwg_layer": "PNTS",
                "locked": "No",
                "group_name": "",
                "category": "Topographic",
                "ls_number": "",
                "source_file": "/repo/samples/input/mixed.csv",
                "source_line": "5",
            },
            {
                "point_id": "4",
                "northing": "1003",
                "easting": "2003",
                "elevation": "303",
                "description": "TOP SHOULDER",
                "dwg_description": "TOP SHOULDER",
                "dwg_layer": "PNTS",
                "locked": "No",
                "group_name": "",
                "category": "Topographic",
                "ls_number": "",
                "source_file": "/repo/samples/input/mixed.csv",
                "source_line": "6",
            },
            {
                "point_id": "5",
                "northing": "1004",
                "easting": "2004",
                "elevation": "304",
                "description": "UNKNOWN TOKEN",
                "dwg_description": "UNKNOWN TOKEN",
                "dwg_layer": "PNTS",
                "locked": "No",
                "group_name": "",
                "category": "Other",
                "ls_number": "",
                "source_file": "/repo/samples/input/mixed.csv",
                "source_line": "7",
            },
        ],
    )

    _write_dataset_snapshot(run_root / "manifest/dataset_snapshot.json", "sha256:test-snapshot")
    _write_run_manifest(run_root / "manifest/run_manifest.json")

    result = run_bridge(run_root=run_root, rules_path=rules_path)

    assert result.mapped_points == 4
    assert result.unmapped_points == 1
    assert result.intent_features == 3
    assert result.geometry_features == 3

    intent_payload = json.loads((run_root / "manifest/intent_ir.json").read_text(encoding="utf-8"))
    geometry_payload = json.loads((run_root / "manifest/geometry_ir.json").read_text(encoding="utf-8"))
    bridge_manifest = json.loads((run_root / "manifest/bridge_manifest.json").read_text(encoding="utf-8"))

    _assert_contract_key_order(intent_payload)
    _assert_contract_key_order(geometry_payload)
    _assert_contract_key_order(bridge_manifest)

    assert intent_payload["artifactType"] == "intent_ir"
    assert geometry_payload["artifactType"] == "geometry_ir"
    assert bridge_manifest["artifactType"] == "bridge_manifest"

    for payload in [intent_payload, geometry_payload, bridge_manifest]:
        for value in _iter_path_values(payload["paths"]):
            assert not is_absolute_path_value(value)

    assert intent_payload["data"]["mapped_points"] == 4
    assert intent_payload["data"]["unmapped_points"] == 1
    feature_codes = [item["feature_code"] for item in intent_payload["data"]["features"]]
    assert "top_shoulder" in feature_codes
    assert "top_generic" not in feature_codes

    line_features = [
        item for item in geometry_payload["data"]["features"] if item["geometry_type"] == "LineString"
    ]
    assert len(line_features) == 1
    assert line_features[0]["topology"]["is_valid"] is True
    assert line_features[0]["topology"]["length_2d"] is not None

    hash_payload = bridge_manifest["data"]["hashes"]
    for key in ["points_sha256", "rule_pack_sha256", "intent_data_sha256", "geometry_data_sha256"]:
        assert len(hash_payload[key]) == 64


def test_bridge_is_deterministic_when_input_rows_are_shuffled(tmp_path: Path) -> None:
    rules_path = tmp_path / "bridge-rules.yaml"
    _write_rule_pack(rules_path)

    canonical_rows = [
        {
            "point_id": "10",
            "northing": "1",
            "easting": "1",
            "elevation": "10",
            "description": "TOP SHOULDER",
            "dwg_description": "TOP SHOULDER",
            "dwg_layer": "PNTS",
            "locked": "No",
            "group_name": "",
            "category": "Topographic",
            "ls_number": "",
            "source_file": "/repo/samples/input/mixed.csv",
            "source_line": "10",
        },
        {
            "point_id": "11",
            "northing": "2",
            "easting": "2",
            "elevation": "10",
            "description": "TOP SHOULDER",
            "dwg_description": "TOP SHOULDER",
            "dwg_layer": "PNTS",
            "locked": "No",
            "group_name": "",
            "category": "Topographic",
            "ls_number": "",
            "source_file": "/repo/samples/input/mixed.csv",
            "source_line": "11",
        },
        {
            "point_id": "12",
            "northing": "3",
            "easting": "3",
            "elevation": "10",
            "description": "CP TEST",
            "dwg_description": "CP TEST",
            "dwg_layer": "PNTS",
            "locked": "No",
            "group_name": "",
            "category": "Control",
            "ls_number": "",
            "source_file": "/repo/samples/input/points.csv",
            "source_line": "12",
        },
    ]

    run_a = tmp_path / "run-a"
    run_b = tmp_path / "run-b"

    _write_points_csv(run_a / "normalized/points.csv", canonical_rows)
    _write_points_csv(run_b / "normalized/points.csv", list(reversed(canonical_rows)))

    _write_dataset_snapshot(run_a / "manifest/dataset_snapshot.json", "sha256:test-snapshot")
    _write_dataset_snapshot(run_b / "manifest/dataset_snapshot.json", "sha256:test-snapshot")
    _write_run_manifest(run_a / "manifest/run_manifest.json")
    _write_run_manifest(run_b / "manifest/run_manifest.json")

    run_bridge(run_root=run_a, rules_path=rules_path)
    run_bridge(run_root=run_b, rules_path=rules_path)

    intent_a = json.loads((run_a / "manifest/intent_ir.json").read_text(encoding="utf-8"))
    intent_b = json.loads((run_b / "manifest/intent_ir.json").read_text(encoding="utf-8"))
    geometry_a = json.loads((run_a / "manifest/geometry_ir.json").read_text(encoding="utf-8"))
    geometry_b = json.loads((run_b / "manifest/geometry_ir.json").read_text(encoding="utf-8"))
    bridge_a = json.loads((run_a / "manifest/bridge_manifest.json").read_text(encoding="utf-8"))
    bridge_b = json.loads((run_b / "manifest/bridge_manifest.json").read_text(encoding="utf-8"))

    assert intent_a["data"] == intent_b["data"]
    assert geometry_a["data"] == geometry_b["data"]
    assert bridge_a["data"]["hashes"] == bridge_b["data"]["hashes"]


def test_bridge_quarantines_ambiguous_matches_at_same_precedence(tmp_path: Path) -> None:
    run_root = tmp_path / "run-ambiguous"
    rules_path = tmp_path / "ambiguous-rules.yaml"
    _write_ambiguous_rule_pack(rules_path)
    _write_points_csv(
        run_root / "normalized/points.csv",
        [
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
        ],
    )
    _write_dataset_snapshot(run_root / "manifest/dataset_snapshot.json", "sha256:test-snapshot")
    _write_run_manifest(run_root / "manifest/run_manifest.json")

    result = run_bridge(run_root=run_root, rules_path=rules_path)
    assert result.mapped_points == 0
    assert result.quarantined_points == 1

    intent_payload = json.loads((run_root / "manifest/intent_ir.json").read_text(encoding="utf-8"))
    assert intent_payload["data"]["routing"]["quarantined"]["count"] == 1
    record = intent_payload["data"]["routing"]["quarantined"]["records"][0]
    assert record["reason"] == "ambiguous_match"
    assert record["candidate_rule_ids"] == ["cp_a", "cp_b"]


def test_bridge_quarantines_topology_invalid_features(tmp_path: Path) -> None:
    run_root = tmp_path / "run-topology"
    rules_path = tmp_path / "topology-rules.yaml"
    _write_line_requires_two_points_rule_pack(rules_path)
    _write_points_csv(
        run_root / "normalized/points.csv",
        [
            {
                "point_id": "1",
                "northing": "1000",
                "easting": "2000",
                "elevation": "300",
                "description": "TOP SOLO",
                "dwg_description": "TOP SOLO",
                "dwg_layer": "PNTS",
                "locked": "No",
                "group_name": "",
                "category": "Topographic",
                "ls_number": "",
                "source_file": "/repo/samples/input/mixed.csv",
                "source_line": "2",
            }
        ],
    )
    _write_dataset_snapshot(run_root / "manifest/dataset_snapshot.json", "sha256:test-snapshot")
    _write_run_manifest(run_root / "manifest/run_manifest.json")

    result = run_bridge(run_root=run_root, rules_path=rules_path)
    assert result.mapped_points == 0
    assert result.quarantined_points == 1
    assert result.geometry_features == 0

    intent_payload = json.loads((run_root / "manifest/intent_ir.json").read_text(encoding="utf-8"))
    quarantined = intent_payload["data"]["routing"]["quarantined"]["records"]
    assert len(quarantined) == 1
    assert quarantined[0]["reason"] == "topology_invalid"
