from __future__ import annotations

from copy import deepcopy
from pathlib import Path
import re
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

QC_PROFILE_OVERRIDES: dict[str, dict[str, Any]] = {
    "strict": {
        "input": {
            "exclude_globs": ["artifacts/**/*"],
        },
        "validation": {
            "max_warning_count": None,
            "duplicate_point_id_mode": "all_occurrences",
            "unmapped_description_skip_categories": [],
            "trend_tracking": {
                "enabled": True,
                "fail_on_spike": True,
                "max_warning_delta": 100,
                "max_error_delta": 0,
                "max_critical_delta": 0,
                "baseline_manifest_path": None,
                "state_file_path": None,
            },
        },
        "remediation": {
            "enabled": False,
            "fix_blank_field_codes": False,
            "drop_duplicate_tail_blocks": False,
            "drop_malformed_footer_rows": False,
        },
    },
    "standard": {
        "input": {
            "exclude_globs": [
                "artifacts/**/*",
                "TOTaLi/IIII.dxf",
                "TOTaLi/XR23173-Sur.dxf",
                ".local-datasets/TOTaLi/IIII.dxf",
                ".local-datasets/TOTaLi/XR23173-Sur.dxf",
            ],
        },
        "validation": {
            "max_warning_count": 5000,
            "duplicate_point_id_mode": "within_file",
            "unmapped_description_skip_categories": ["Converted"],
            "trend_tracking": {
                "enabled": True,
                "fail_on_spike": True,
                "max_warning_delta": 200,
                "max_error_delta": 0,
                "max_critical_delta": 0,
                "baseline_manifest_path": None,
                "state_file_path": None,
            },
        },
        "remediation": {
            "enabled": True,
            "fix_blank_field_codes": True,
            "drop_duplicate_tail_blocks": False,
            "drop_malformed_footer_rows": False,
        },
    },
    "legacy": {
        "input": {
            "exclude_globs": ["artifacts/**/*"],
        },
        "validation": {
            "max_warning_count": 10000,
            "duplicate_point_id_mode": "per_point_id",
            "unmapped_description_skip_categories": ["Converted", "Legacy"],
            "trend_tracking": {
                "enabled": True,
                "fail_on_spike": True,
                "max_warning_delta": 1000,
                "max_error_delta": 5,
                "max_critical_delta": 0,
                "baseline_manifest_path": None,
                "state_file_path": None,
            },
        },
        "remediation": {
            "enabled": True,
            "fix_blank_field_codes": True,
            "drop_duplicate_tail_blocks": True,
            "drop_malformed_footer_rows": True,
        },
    },
}

SUPPORTED_QC_PROFILES = tuple(sorted(QC_PROFILE_OVERRIDES))
SUPPORTED_CRD_MODES = ("auto", "converter_required", "text_only")
SUPPORTED_COLOR_CATEGORIES = ("config", "data", "environment", "converter")
SUPPORTED_PRESENTATION_COLOR_BASIS = "category_config"
_HEX_COLOR_RE = re.compile(r"^#[0-9A-Fa-f]{6}$")

DEFAULT_CONFIG: dict[str, Any] = {
    "version": "1",
    "project": {
        "qc_profile": "strict",
        "baseline_namespace": None,
    },
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
        "trend_tracking": {
            "enabled": True,
            "fail_on_spike": True,
            "max_warning_delta": 100,
            "max_error_delta": 0,
            "max_critical_delta": 0,
            "baseline_manifest_path": None,
            "state_file_path": None,
        },
    },
    "remediation": {
        "enabled": False,
        "fix_blank_field_codes": False,
        "drop_duplicate_tail_blocks": False,
        "drop_malformed_footer_rows": False,
    },
    "crd": {
        "mode": "auto",
        "converter_command": None,
        "converter_failure_mode": "fatal",
    },
    "outputs": {
        "formats": ["csv", "parquet"],
    },
    "presentation": {
        "enabled": True,
        "color_basis": "category_config",
        "category_colors": {
            "config": "#0072B2",
            "data": "#009E73",
            "environment": "#D55E00",
            "converter": "#CC79A7",
        },
        "config_colors": {
            "qc_profile": {
                "strict": "#0072B2",
                "standard": "#56B4E9",
                "legacy": "#E69F00",
            },
            "crd_mode": {
                "auto": "#009E73",
                "converter_required": "#D55E00",
                "text_only": "#999999",
            },
        },
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


def _require_hex_color(value: Any, key_path: str) -> str:
    if not isinstance(value, str) or _HEX_COLOR_RE.fullmatch(value) is None:
        raise ConfigError(f"`{key_path}` must be a 6-digit hex color (#RRGGBB)")
    return value


def load_config(config_path: str | Path) -> dict[str, Any]:
    path = Path(config_path)
    if not path.exists():
        raise ConfigError(f"Config file does not exist: {path}")

    raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(raw, dict):
        raise ConfigError("Config root must be a YAML mapping")

    if "version" not in raw or not isinstance(raw.get("version"), str) or not raw["version"].strip():
        raise ConfigError("Config key `version` is required and must be a non-empty string")

    raw_project = raw.get("project", {})
    if raw_project is None:
        raw_project = {}
    if not isinstance(raw_project, dict):
        raise ConfigError("`project` must be a mapping when provided")
    raw_profile = raw_project.get("qc_profile", DEFAULT_CONFIG["project"]["qc_profile"])
    if not isinstance(raw_profile, str) or not raw_profile.strip():
        raise ConfigError(
            "`project.qc_profile` is required and must be one of: " + ", ".join(SUPPORTED_QC_PROFILES)
        )
    qc_profile = raw_profile.strip().lower()
    if qc_profile not in SUPPORTED_QC_PROFILES:
        raise ConfigError(
            "`project.qc_profile` must be one of: " + ", ".join(SUPPORTED_QC_PROFILES)
        )

    raw_baseline_namespace = raw_project.get("baseline_namespace")
    if raw_baseline_namespace is not None and not isinstance(raw_baseline_namespace, str):
        raise ConfigError("`project.baseline_namespace` must be a string or null")

    profile_overrides = QC_PROFILE_OVERRIDES[qc_profile]
    merged = _deep_merge(DEFAULT_CONFIG, profile_overrides)
    merged = _deep_merge(merged, raw)
    merged["project"]["qc_profile"] = qc_profile
    baseline_namespace = merged.get("project", {}).get("baseline_namespace")
    if isinstance(baseline_namespace, str):
        merged["project"]["baseline_namespace"] = baseline_namespace.strip() or None

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
    if mode not in SUPPORTED_CRD_MODES:
        raise ConfigError("`crd.mode` must be one of: auto, converter_required, text_only")

    converter_failure_mode = merged.get("crd", {}).get("converter_failure_mode")
    if converter_failure_mode not in {"fatal", "quarantine"}:
        raise ConfigError("`crd.converter_failure_mode` must be one of: fatal, quarantine")

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

    for bool_path in [
        ("remediation", "enabled"),
        ("remediation", "fix_blank_field_codes"),
        ("remediation", "drop_duplicate_tail_blocks"),
        ("remediation", "drop_malformed_footer_rows"),
    ]:
        ref = merged
        for key in bool_path[:-1]:
            ref = ref[key]
        value = ref[bool_path[-1]]
        if not isinstance(value, bool):
            raise ConfigError(f"`{'.'.join(bool_path)}` must be a boolean")

    trend_cfg = merged.get("validation", {}).get("trend_tracking")
    if not isinstance(trend_cfg, dict):
        raise ConfigError("`validation.trend_tracking` must be a mapping")
    for trend_bool_key in ("enabled", "fail_on_spike"):
        if not isinstance(trend_cfg.get(trend_bool_key), bool):
            raise ConfigError(f"`validation.trend_tracking.{trend_bool_key}` must be a boolean")
    for trend_int_key in ("max_warning_delta", "max_error_delta", "max_critical_delta"):
        trend_value = trend_cfg.get(trend_int_key)
        if trend_value is not None and (not isinstance(trend_value, int) or trend_value < 0):
            raise ConfigError(
                f"`validation.trend_tracking.{trend_int_key}` must be an integer >= 0 or null"
            )
    for trend_path_key in ("baseline_manifest_path", "state_file_path"):
        trend_value = trend_cfg.get(trend_path_key)
        if trend_value is not None and not isinstance(trend_value, str):
            raise ConfigError(
                f"`validation.trend_tracking.{trend_path_key}` must be a string path or null"
            )

    presentation_cfg = merged.get("presentation")
    if not isinstance(presentation_cfg, dict):
        raise ConfigError("`presentation` must be a mapping")

    presentation_enabled = presentation_cfg.get("enabled")
    if not isinstance(presentation_enabled, bool):
        raise ConfigError("`presentation.enabled` must be a boolean")

    color_basis = presentation_cfg.get("color_basis")
    if color_basis != SUPPORTED_PRESENTATION_COLOR_BASIS:
        raise ConfigError(
            f"`presentation.color_basis` must be: {SUPPORTED_PRESENTATION_COLOR_BASIS}"
        )

    category_colors = presentation_cfg.get("category_colors")
    if not isinstance(category_colors, dict):
        raise ConfigError("`presentation.category_colors` must be a mapping")
    unknown_categories = sorted(set(category_colors) - set(SUPPORTED_COLOR_CATEGORIES))
    if unknown_categories:
        raise ConfigError(
            "`presentation.category_colors` supports only: " + ", ".join(SUPPORTED_COLOR_CATEGORIES)
        )
    for category_name in SUPPORTED_COLOR_CATEGORIES:
        if category_name not in category_colors:
            raise ConfigError(
                "`presentation.category_colors` is missing required key: " + category_name
            )
        _require_hex_color(category_colors[category_name], f"presentation.category_colors.{category_name}")

    config_colors = presentation_cfg.get("config_colors")
    if not isinstance(config_colors, dict):
        raise ConfigError("`presentation.config_colors` must be a mapping")
    unknown_config_color_sections = sorted(set(config_colors) - {"qc_profile", "crd_mode"})
    if unknown_config_color_sections:
        raise ConfigError("`presentation.config_colors` supports only: qc_profile, crd_mode")

    qc_profile_colors = config_colors.get("qc_profile")
    if not isinstance(qc_profile_colors, dict):
        raise ConfigError("`presentation.config_colors.qc_profile` must be a mapping")
    unknown_qc_profiles = sorted(set(qc_profile_colors) - set(SUPPORTED_QC_PROFILES))
    if unknown_qc_profiles:
        raise ConfigError(
            "`presentation.config_colors.qc_profile` supports only: " + ", ".join(SUPPORTED_QC_PROFILES)
        )
    for profile_name in SUPPORTED_QC_PROFILES:
        if profile_name not in qc_profile_colors:
            raise ConfigError(
                "`presentation.config_colors.qc_profile` is missing required key: " + profile_name
            )
        _require_hex_color(
            qc_profile_colors[profile_name],
            f"presentation.config_colors.qc_profile.{profile_name}",
        )

    crd_mode_colors = config_colors.get("crd_mode")
    if not isinstance(crd_mode_colors, dict):
        raise ConfigError("`presentation.config_colors.crd_mode` must be a mapping")
    unknown_crd_modes = sorted(set(crd_mode_colors) - set(SUPPORTED_CRD_MODES))
    if unknown_crd_modes:
        raise ConfigError(
            "`presentation.config_colors.crd_mode` supports only: " + ", ".join(SUPPORTED_CRD_MODES)
        )
    for crd_mode in SUPPORTED_CRD_MODES:
        if crd_mode not in crd_mode_colors:
            raise ConfigError(
                "`presentation.config_colors.crd_mode` is missing required key: " + crd_mode
            )
        _require_hex_color(
            crd_mode_colors[crd_mode],
            f"presentation.config_colors.crd_mode.{crd_mode}",
        )

    return merged


def write_example_config(path: str | Path) -> None:
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(yaml.safe_dump(DEFAULT_CONFIG, sort_keys=False), encoding="utf-8")
