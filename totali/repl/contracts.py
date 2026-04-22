"""Deterministic contract helpers for REPL request/response payloads."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Mapping, Sequence

SCHEMA_VERSION = "1.0.0"

CONTRACT_TOP_LEVEL_KEYS: tuple[str, ...] = (
    "schemaVersion",
    "artifactType",
    "invariants",
    "metadata",
    "paths",
    "data",
)

BASE_INVARIANTS: tuple[str, ...] = (
    "schema_version_required",
    "deterministic_key_order",
    "offline_first_runtime",
    "no_runtime_cloud_apis",
)

_WINDOWS_ABSOLUTE_PATH_RE = re.compile(r"^(?:[A-Za-z]:[\\/]|\\\\)")


def _is_absolute_path(value: str) -> bool:
    if value.startswith("/"):
        return True
    return _WINDOWS_ABSOLUTE_PATH_RE.match(value) is not None


def _has_path_traversal(value: str) -> bool:
    normalized = value.replace("\\", "/")
    return any(part == ".." for part in Path(normalized).parts)


def _validate_metadata(metadata: Mapping[str, Any]) -> dict[str, Any]:
    normalized: dict[str, Any] = {}
    for key, value in metadata.items():
        if not isinstance(key, str) or not key:
            raise ValueError("metadata keys must be non-empty strings")
        if not isinstance(value, (str, int, float, bool)) and value is not None:
            raise ValueError(
                f"metadata.{key} must be scalar (string/number/boolean/null), got {type(value)}"
            )
        normalized[key] = value
    return normalized


def _iter_path_values(node: Any, *, prefix: str = "") -> list[tuple[str, str]]:
    values: list[tuple[str, str]] = []
    if isinstance(node, str):
        values.append((prefix or "<root>", node))
        return values
    if isinstance(node, dict):
        for key, value in node.items():
            child_prefix = f"{prefix}.{key}" if prefix else str(key)
            values.extend(_iter_path_values(value, prefix=child_prefix))
        return values
    if isinstance(node, list):
        for index, value in enumerate(node):
            child_prefix = f"{prefix}[{index}]" if prefix else f"[{index}]"
            values.extend(_iter_path_values(value, prefix=child_prefix))
        return values
    return values


def _validate_paths(paths: Mapping[str, Any]) -> dict[str, Any]:
    normalized = dict(paths)
    for key_path, value in _iter_path_values(normalized):
        if not isinstance(value, str):
            raise ValueError(f"paths.{key_path} must be a string")
        if not value:
            raise ValueError(f"paths.{key_path} must not be empty")
        if _is_absolute_path(value):
            raise ValueError(f"absolute path is not allowed in paths.{key_path}: {value}")
        if _has_path_traversal(value):
            raise ValueError(f"path traversal is not allowed in paths.{key_path}: {value}")
    return normalized


def _normalize_invariants(invariants: Sequence[str] | None) -> list[str]:
    ordered: list[str] = list(BASE_INVARIANTS)
    if not invariants:
        return ordered
    for item in invariants:
        if not isinstance(item, str) or not item.strip():
            raise ValueError("invariants must be non-empty strings")
        if item not in ordered:
            ordered.append(item)
    return ordered


def build_contract_payload(
    *,
    artifact_type: str,
    data: Mapping[str, Any] | None = None,
    metadata: Mapping[str, Any] | None = None,
    paths: Mapping[str, Any] | None = None,
    invariants: Sequence[str] | None = None,
    schema_version: str = SCHEMA_VERSION,
) -> dict[str, Any]:
    if not isinstance(schema_version, str) or not schema_version.strip():
        raise ValueError("schema_version must be a non-empty string")
    if not isinstance(artifact_type, str) or not artifact_type.strip():
        raise ValueError("artifact_type must be a non-empty string")

    payload: dict[str, Any] = {}
    payload["schemaVersion"] = schema_version
    payload["artifactType"] = artifact_type
    payload["invariants"] = _normalize_invariants(invariants)
    payload["metadata"] = _validate_metadata(metadata or {})
    payload["paths"] = _validate_paths(paths or {})
    payload["data"] = dict(data or {})
    return payload


def validate_contract_payload(
    payload: Mapping[str, Any],
    *,
    expected_artifact_type: str | None = None,
) -> list[str]:
    errors: list[str] = []
    keys = list(payload.keys())
    if keys != list(CONTRACT_TOP_LEVEL_KEYS):
        errors.append(
            "Top-level keys must be exactly ordered as: "
            + ", ".join(CONTRACT_TOP_LEVEL_KEYS)
        )

    schema_version = payload.get("schemaVersion")
    if not isinstance(schema_version, str) or not schema_version.strip():
        errors.append("schemaVersion must be a non-empty string")

    artifact_type = payload.get("artifactType")
    if not isinstance(artifact_type, str) or not artifact_type.strip():
        errors.append("artifactType must be a non-empty string")
    elif expected_artifact_type and artifact_type != expected_artifact_type:
        errors.append(
            f"artifactType expected `{expected_artifact_type}` but found `{artifact_type}`"
        )

    invariants = payload.get("invariants")
    if not isinstance(invariants, list) or not invariants:
        errors.append("invariants must be a non-empty list")
    else:
        seen: set[str] = set()
        for index, item in enumerate(invariants):
            if not isinstance(item, str) or not item.strip():
                errors.append(f"invariants[{index}] must be a non-empty string")
                continue
            if item in seen:
                errors.append(f"invariants contains duplicate value: {item}")
            seen.add(item)
        for base in BASE_INVARIANTS:
            if base not in seen:
                errors.append(f"missing base invariant: {base}")

    metadata = payload.get("metadata")
    if not isinstance(metadata, dict):
        errors.append("metadata must be an object")
    else:
        try:
            _validate_metadata(metadata)
        except ValueError as exc:
            errors.append(str(exc))

    paths = payload.get("paths")
    if not isinstance(paths, dict):
        errors.append("paths must be an object")
    else:
        try:
            _validate_paths(paths)
        except ValueError as exc:
            errors.append(str(exc))

    data = payload.get("data")
    if not isinstance(data, dict):
        errors.append("data must be an object")

    return errors


def canonical_json(payload: Mapping[str, Any]) -> str:
    """Serialize payload with deterministic ordering and compact separators."""
    ordered_payload = {key: payload.get(key) for key in CONTRACT_TOP_LEVEL_KEYS}
    errors = validate_contract_payload(ordered_payload)
    if errors:
        raise ValueError("Invalid contract payload: " + "; ".join(errors))
    return json.dumps(ordered_payload, ensure_ascii=True, separators=(",", ":"))
