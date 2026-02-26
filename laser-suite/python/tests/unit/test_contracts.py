from pathlib import Path

import pytest

from laser_suite.contracts import build_contract_payload, validate_top_level_order


def test_contract_key_order() -> None:
    payload = build_contract_payload(
        artifact_type="test_artifact",
        invariants=["deterministic_key_order"],
        metadata={"run": "x"},
        paths={"file": "relative/path.csv"},
        data={"ok": True},
    )
    validate_top_level_order(payload)
    assert list(payload.keys()) == [
        "schemaVersion",
        "artifactType",
        "invariants",
        "metadata",
        "paths",
        "data",
    ]


def test_contract_rejects_absolute_paths() -> None:
    with pytest.raises(ValueError):
        build_contract_payload(
            artifact_type="test_artifact",
            invariants=["deterministic_key_order"],
            metadata={"run": "x"},
            paths={"file": "/absolute/path.csv"},
            data={"ok": True},
        )
