from __future__ import annotations

from copy import deepcopy
from pathlib import Path
from typing import Any

import yaml


DEFAULT_CONFIG: dict[str, Any] = {
    "schemaVersion": "1.0.0",
    "laser": {
        "adjustment": {
            "max_iterations": 12,
            "convergence_tol": 1e-6,
            "condition_number_limit": 1e15,
            "svd_rcond": 1e-12,
        },
        "rpp": {
            "k95": 2.448,
            "allowable_base_m": 0.02,
            "allowable_ppm": 50.0,
        },
    },
    "encroachment": {
        "snap_tolerance_m": 0.001,
        "depth_method": "max_vertex_distance",
    },
    "contracts": {
        "require_schema_version": True,
        "require_relative_paths": True,
        "deterministic_key_order": True,
    },
    "civil3d": {
        "queue": {"max_items": 1000},
        "notifications": {"enable": True},
    },
}


class ConfigError(ValueError):
    pass


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged = deepcopy(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def _as_float(value: Any, field: str) -> float:
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError as exc:
            raise ConfigError(f"{field} must be numeric") from exc
    raise ConfigError(f"{field} must be numeric")


def _as_int(value: Any, field: str) -> int:
    if isinstance(value, bool):
        raise ConfigError(f"{field} must be an integer")
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError as exc:
            raise ConfigError(f"{field} must be an integer") from exc
    raise ConfigError(f"{field} must be an integer")


def load_config(path: Path) -> dict[str, Any]:
    raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(raw, dict):
        raise ConfigError("Config root must be a mapping")
    config = _deep_merge(DEFAULT_CONFIG, raw)

    adjustment = config["laser"]["adjustment"]
    adjustment["max_iterations"] = _as_int(
        adjustment["max_iterations"], "laser.adjustment.max_iterations"
    )
    adjustment["convergence_tol"] = _as_float(
        adjustment["convergence_tol"], "laser.adjustment.convergence_tol"
    )
    adjustment["condition_number_limit"] = _as_float(
        adjustment["condition_number_limit"], "laser.adjustment.condition_number_limit"
    )
    adjustment["svd_rcond"] = _as_float(
        adjustment["svd_rcond"], "laser.adjustment.svd_rcond"
    )

    if adjustment["max_iterations"] < 1:
        raise ConfigError("laser.adjustment.max_iterations must be >= 1")
    if adjustment["convergence_tol"] <= 0:
        raise ConfigError("laser.adjustment.convergence_tol must be > 0")
    if adjustment["condition_number_limit"] <= 0:
        raise ConfigError("laser.adjustment.condition_number_limit must be > 0")

    rpp = config["laser"]["rpp"]
    rpp["k95"] = _as_float(rpp["k95"], "laser.rpp.k95")
    rpp["allowable_base_m"] = _as_float(rpp["allowable_base_m"], "laser.rpp.allowable_base_m")
    rpp["allowable_ppm"] = _as_float(rpp["allowable_ppm"], "laser.rpp.allowable_ppm")
    if rpp["k95"] <= 0:
        raise ConfigError("laser.rpp.k95 must be > 0")
    if rpp["allowable_base_m"] < 0:
        raise ConfigError("laser.rpp.allowable_base_m must be >= 0")
    if rpp["allowable_ppm"] < 0:
        raise ConfigError("laser.rpp.allowable_ppm must be >= 0")

    snap_tol = _as_float(
        config["encroachment"]["snap_tolerance_m"],
        "encroachment.snap_tolerance_m",
    )
    config["encroachment"]["snap_tolerance_m"] = snap_tol
    if snap_tol <= 0:
        raise ConfigError("encroachment.snap_tolerance_m must be > 0")

    return config
