from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Any

CANONICAL_SCHEMA_VERSION = "2.0.0"

CONTRACT_TOP_LEVEL_KEYS = (
    "schemaVersion",
    "artifactType",
    "invariants",
    "metadata",
    "paths",
    "data",
)

BASE_INVARIANT_NAMES = (
    "paths_are_relative",
    "deterministic_key_order",
    "typed_sections_only",
    "no_ambiguous_types",
)

_WINDOWS_ABSOLUTE_PATH_RE = re.compile(r"^(?:[A-Za-z]:[\\/]|\\\\)")


def is_absolute_path_value(value: str) -> bool:
    if value.startswith("/"):
        return True
    if _WINDOWS_ABSOLUTE_PATH_RE.match(value) is not None:
        return True
    return False


def has_path_traversal(value: str) -> bool:
    parts = Path(value.replace("\\", "/")).parts
    return any(part == ".." for part in parts)


def to_stable_relative_path(path: Path, *, base: Path) -> str:
    resolved_path = path.resolve()
    resolved_base = base.resolve()
    try:
        relative = resolved_path.relative_to(resolved_base).as_posix()
    except ValueError:
        relative = Path(os.path.relpath(str(resolved_path), str(resolved_base))).as_posix()
    return relative


def build_invariant(name: str, *, passed: bool = True, detail: str = "") -> dict[str, Any]:
    return {
        "name": name,
        "passed": bool(passed),
        "detail": str(detail),
    }


def normalize_invariants(invariants: list[dict[str, Any] | str] | None) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    seen_names: set[str] = set()

    for name in BASE_INVARIANT_NAMES:
        seen_names.add(name)
        normalized.append(build_invariant(name))

    if not invariants:
        return normalized

    for item in invariants:
        if isinstance(item, str):
            name = item.strip()
            if not name:
                continue
            if name in seen_names:
                continue
            seen_names.add(name)
            normalized.append(build_invariant(name))
            continue

        if not isinstance(item, dict):
            raise ValueError("Invariant entries must be strings or objects")
        name = item.get("name")
        if not isinstance(name, str) or not name.strip():
            raise ValueError("Invariant object requires non-empty `name`")
        if name in seen_names:
            continue
        seen_names.add(name)
        normalized.append(
            build_invariant(
                name=name,
                passed=bool(item.get("passed", True)),
                detail=str(item.get("detail", "")),
            )
        )

    return normalized


def build_contract_payload(
    *,
    artifact_type: str,
    invariants: list[dict[str, Any] | str] | None = None,
    metadata: dict[str, Any] | None = None,
    paths: dict[str, Any] | None = None,
    data: dict[str, Any] | None = None,
    schema_version: str = CANONICAL_SCHEMA_VERSION,
) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    payload["schemaVersion"] = schema_version
    payload["artifactType"] = artifact_type
    payload["invariants"] = normalize_invariants(invariants)
    payload["metadata"] = metadata if metadata is not None else {}
    payload["paths"] = paths if paths is not None else {}
    payload["data"] = data if data is not None else {}
    return payload


def _is_scalar(value: Any) -> bool:
    return isinstance(value, (str, int, float, bool)) or value is None


def iter_path_values(node: Any, prefix: str = "") -> list[tuple[str, str]]:
    values: list[tuple[str, str]] = []
    if isinstance(node, str):
        values.append((prefix or "<root>", node))
        return values
    if isinstance(node, dict):
        for key, value in node.items():
            child_prefix = f"{prefix}.{key}" if prefix else str(key)
            values.extend(iter_path_values(value, child_prefix))
        return values
    if isinstance(node, list):
        for index, value in enumerate(node):
            child_prefix = f"{prefix}[{index}]" if prefix else f"[{index}]"
            values.extend(iter_path_values(value, child_prefix))
        return values
    return values


def validate_contract_top_level(payload: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    keys = list(payload.keys())
    if keys != list(CONTRACT_TOP_LEVEL_KEYS):
        errors.append(
            "Top-level keys must be exactly ordered as: "
            + ", ".join(CONTRACT_TOP_LEVEL_KEYS)
        )
    return errors


def validate_contract_sections(
    payload: dict[str, Any],
    *,
    expected_artifact_type: str | None = None,
    require_canonical_schema: bool = True,
) -> list[str]:
    errors: list[str] = []
    errors.extend(validate_contract_top_level(payload))

    schema_version = payload.get("schemaVersion")
    if not isinstance(schema_version, str) or not schema_version.strip():
        errors.append("`schemaVersion` must be a non-empty string")
    elif require_canonical_schema and schema_version != CANONICAL_SCHEMA_VERSION:
        errors.append(
            f"`schemaVersion` must equal {CANONICAL_SCHEMA_VERSION} (found {schema_version})"
        )

    artifact_type = payload.get("artifactType")
    if not isinstance(artifact_type, str) or not artifact_type.strip():
        errors.append("`artifactType` must be a non-empty string")
    elif expected_artifact_type and artifact_type != expected_artifact_type:
        errors.append(
            f"artifactType expected `{expected_artifact_type}` but found `{artifact_type}`"
        )

    invariants = payload.get("invariants")
    if not isinstance(invariants, list):
        errors.append("`invariants` must be a list")
    else:
        seen_names: set[str] = set()
        for index, item in enumerate(invariants):
            if not isinstance(item, dict):
                errors.append(f"`invariants[{index}]` must be an object")
                continue
            name = item.get("name")
            passed = item.get("passed")
            detail = item.get("detail")
            if not isinstance(name, str) or not name.strip():
                errors.append(f"`invariants[{index}].name` must be a non-empty string")
            elif name in seen_names:
                errors.append(f"`invariants` contains duplicate name: {name}")
            else:
                seen_names.add(name)
            if not isinstance(passed, bool):
                errors.append(f"`invariants[{index}].passed` must be a boolean")
            if not isinstance(detail, str):
                errors.append(f"`invariants[{index}].detail` must be a string")
        missing_base = [name for name in BASE_INVARIANT_NAMES if name not in seen_names]
        if missing_base:
            errors.append("Missing base invariants: " + ", ".join(missing_base))

    metadata = payload.get("metadata")
    if not isinstance(metadata, dict):
        errors.append("`metadata` must be an object")
    else:
        for key, value in metadata.items():
            if not _is_scalar(value):
                errors.append(
                    f"`metadata.{key}` must be scalar (string/number/boolean/null)"
                )

    paths = payload.get("paths")
    if not isinstance(paths, dict):
        errors.append("`paths` must be an object")
    else:
        for key_path, value in iter_path_values(paths):
            if not isinstance(value, str):
                errors.append(f"`paths.{key_path}` must be a string")
                continue
            if not value:
                errors.append(f"`paths.{key_path}` must not be empty")
                continue
            if is_absolute_path_value(value):
                errors.append(f"absolute path detected in `paths.{key_path}`: {value}")
            if has_path_traversal(value):
                errors.append(f"path traversal detected in `paths.{key_path}`: {value}")

    data = payload.get("data")
    if not isinstance(data, dict):
        errors.append("`data` must be an object")

    return errors


def write_contract_json(path: Path, payload: dict[str, Any]) -> None:
    ordered_payload = {
        key: payload.get(key)
        for key in CONTRACT_TOP_LEVEL_KEYS
    }
    errors = validate_contract_sections(ordered_payload, require_canonical_schema=True)
    if errors:
        raise ValueError("Invalid contract payload: " + "; ".join(errors))
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(ordered_payload, indent=2), encoding="utf-8")
