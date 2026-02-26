from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .schemas import SCHEMA_VERSION

TOP_LEVEL_ORDER = [
    "schemaVersion",
    "artifactType",
    "invariants",
    "metadata",
    "paths",
    "data",
]


def _is_absolute_path(value: str) -> bool:
    return value.startswith("/") or (len(value) >= 3 and value[1] == ":" and value[2] in {"/", "\\"})


def _iter_path_values(node: Any):
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


def ensure_relative_paths(paths: dict[str, Any]) -> None:
    for value in _iter_path_values(paths):
        if _is_absolute_path(value):
            raise ValueError(f"Absolute path is not allowed in contract paths: {value}")


def build_contract_payload(
    *,
    artifact_type: str,
    invariants: list[str],
    metadata: dict[str, Any],
    paths: dict[str, Any],
    data: dict[str, Any],
    schema_version: str = SCHEMA_VERSION,
) -> dict[str, Any]:
    if not isinstance(invariants, list) or not all(isinstance(item, str) and item for item in invariants):
        raise ValueError("invariants must be a non-empty list[str]")

    ensure_relative_paths(paths)

    payload: dict[str, Any] = {
        "schemaVersion": schema_version,
        "artifactType": artifact_type,
        "invariants": invariants,
        "metadata": metadata,
        "paths": paths,
        "data": data,
    }
    return payload


def validate_top_level_order(payload: dict[str, Any]) -> None:
    keys = list(payload.keys())
    if keys != TOP_LEVEL_ORDER:
        raise ValueError(f"Invalid top-level ordering: {keys}")


def write_contract_json(path: Path, payload: dict[str, Any]) -> None:
    validate_top_level_order(payload)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=True), encoding="utf-8")
