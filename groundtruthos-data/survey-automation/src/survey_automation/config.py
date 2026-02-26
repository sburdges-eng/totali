from __future__ import annotations

from copy import deepcopy
from pathlib import Path
from typing import Any

import yaml

POINT_HEADERS = [
    "Point#",
    "Northing",
    "Easting",
    "Elevation",
    "Description",
    "DWG Description",
    "DWG Layer",
    "Locked",
    "Group",
    "Category",
    "LS Number",
]

FIELD_CODE_HEADERS = [
    "Field Code",
    "Layer",
    "Symbol",
    "Linework",
]

DEFAULT_CONFIG: dict[str, Any] = {
    "version": "1",
    "input": {
        "include_globs": ["**/*"],
        "exclude_globs": [],
    },
    "normalization": {
        "trim_strings": True,
        "uppercase_field_code": True,
    },
    "validation": {
        "required_point_columns": POINT_HEADERS,
        "required_field_code_columns": FIELD_CODE_HEADERS,
        "fail_if_all_files_invalid": True,
        "coordinate_bounds": None,
        "max_warning_count": None,
        "duplicate_point_id_mode": "all_occurrences",
        "unmapped_description_skip_categories": [],
    },
    "crd": {
        "mode": "auto",
        "converter_command": None,
    },
    "outputs": {
        "formats": ["csv", "parquet"],
    },
}


class ConfigError(ValueError):
    pass


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    result = deepcopy(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(result.get(key), dict):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = value
    return result


def _require_list_of_strings(config: dict[str, Any], key_path: tuple[str, ...]) -> list[str]:
    ref: Any = config
    for key in key_path:
        if not isinstance(ref, dict) or key not in ref:
            raise ConfigError(f"Missing required config key: {'.'.join(key_path)}")
        ref = ref[key]
    if not isinstance(ref, list) or not all(isinstance(i, str) for i in ref):
        raise ConfigError(f"Config key must be list[str]: {'.'.join(key_path)}")
    return ref


def load_config(config_path: str | Path) -> dict[str, Any]:
    path = Path(config_path)
    if not path.exists():
        raise ConfigError(f"Config file does not exist: {path}")

    raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(raw, dict):
        raise ConfigError("Config root must be a YAML mapping")

    if "version" not in raw or not isinstance(raw.get("version"), str) or not raw["version"].strip():
        raise ConfigError("Config key `version` is required and must be a non-empty string")

    merged = _deep_merge(DEFAULT_CONFIG, raw)

    include_globs = _require_list_of_strings(merged, ("input", "include_globs"))
    _require_list_of_strings(merged, ("input", "exclude_globs"))
    point_columns = _require_list_of_strings(merged, ("validation", "required_point_columns"))
    field_code_columns = _require_list_of_strings(merged, ("validation", "required_field_code_columns"))
    output_formats = _require_list_of_strings(merged, ("outputs", "formats"))

    if not include_globs:
        raise ConfigError("`input.include_globs` must include at least one pattern")

    missing_point_columns = sorted(set(POINT_HEADERS) - set(point_columns))
    if missing_point_columns:
        raise ConfigError(
            "`validation.required_point_columns` is missing required columns: "
            + ", ".join(missing_point_columns)
        )
    if len(point_columns) != len(set(point_columns)):
        raise ConfigError("`validation.required_point_columns` contains duplicate column names")

    missing_field_code_columns = sorted(set(FIELD_CODE_HEADERS) - set(field_code_columns))
    if missing_field_code_columns:
        raise ConfigError(
            "`validation.required_field_code_columns` is missing required columns: "
            + ", ".join(missing_field_code_columns)
        )
    if len(field_code_columns) != len(set(field_code_columns)):
        raise ConfigError("`validation.required_field_code_columns` contains duplicate column names")

    mode = merged.get("crd", {}).get("mode")
    if mode not in {"auto", "converter_required", "text_only"}:
        raise ConfigError("`crd.mode` must be one of: auto, converter_required, text_only")

    normalized_formats = []
    for item in output_formats:
        fmt = item.lower().strip()
        if fmt not in {"csv", "parquet"}:
            raise ConfigError("`outputs.formats` supports only: csv, parquet")
        if fmt not in normalized_formats:
            normalized_formats.append(fmt)
    merged["outputs"]["formats"] = normalized_formats

    for bool_path in [
        ("normalization", "trim_strings"),
        ("normalization", "uppercase_field_code"),
        ("validation", "fail_if_all_files_invalid"),
    ]:
        ref = merged
        for key in bool_path[:-1]:
            ref = ref[key]
        value = ref[bool_path[-1]]
        if not isinstance(value, bool):
            raise ConfigError(f"`{'.'.join(bool_path)}` must be a boolean")

    converter_command = merged.get("crd", {}).get("converter_command")
    if converter_command is not None and not isinstance(converter_command, str):
        raise ConfigError("`crd.converter_command` must be a string when provided")

    max_warning_count = merged.get("validation", {}).get("max_warning_count")
    if max_warning_count is not None:
        if not isinstance(max_warning_count, int):
            raise ConfigError("`validation.max_warning_count` must be an integer or null")
        if max_warning_count < 0:
            raise ConfigError("`validation.max_warning_count` must be >= 0 when provided")

    duplicate_mode = merged.get("validation", {}).get("duplicate_point_id_mode")
    if duplicate_mode not in {"all_occurrences", "per_point_id", "within_file"}:
        raise ConfigError(
            "`validation.duplicate_point_id_mode` must be one of: all_occurrences, per_point_id, within_file"
        )

    _require_list_of_strings(merged, ("validation", "unmapped_description_skip_categories"))

    return merged


def write_example_config(path: str | Path) -> None:
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(yaml.safe_dump(DEFAULT_CONFIG, sort_keys=False), encoding="utf-8")
