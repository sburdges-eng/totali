from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .bridge import _build_points_digest, _load_points, _read_input_rel_paths
from .json_contract import (
    build_contract_payload,
    build_invariant,
    has_path_traversal,
    is_absolute_path_value,
    iter_path_values,
    to_stable_relative_path,
    validate_contract_sections,
    write_contract_json,
)


@dataclass(slots=True)
class ArbitrationResult:
    ok: bool
    report_path: Path
    report: dict[str, Any]


def _sha256_json_data(payload: dict[str, Any]) -> str:
    encoded = json.dumps(payload, ensure_ascii=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _sha256_file(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def _load_json_dict(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"Expected JSON object at {path}")
    return payload


def _resolve_in_run_root(run_root: Path, relative_value: str) -> Path:
    if is_absolute_path_value(relative_value):
        raise ValueError(f"Absolute path is not allowed: {relative_value}")
    if has_path_traversal(relative_value):
        raise ValueError(f"Path traversal is not allowed: {relative_value}")
    return (run_root / Path(relative_value)).resolve()


def _iter_required_artifacts(run_root: Path, require_bridge: bool) -> list[tuple[str, Path, str]]:
    artifacts: list[tuple[str, Path, str]] = [
        ("run_manifest", run_root / "manifest" / "run_manifest.json", "run_manifest"),
        ("dataset_snapshot", run_root / "manifest" / "dataset_snapshot.json", "dataset_snapshot"),
        ("qc_summary", run_root / "reports" / "qc_summary.json", "qc_summary"),
        ("qc_trend", run_root / "reports" / "qc_trend.json", "qc_trend"),
        ("quarantined_files", run_root / "quarantine" / "quarantined_files.json", "quarantined_files"),
    ]
    if require_bridge:
        artifacts.extend(
            [
                ("intent_ir", run_root / "manifest" / "intent_ir.json", "intent_ir"),
                ("geometry_ir", run_root / "manifest" / "geometry_ir.json", "geometry_ir"),
                ("bridge_manifest", run_root / "manifest" / "bridge_manifest.json", "bridge_manifest"),
            ]
        )
    return artifacts


def _append_violation(violations: list[dict[str, Any]], code: str, message: str, artifact: str) -> None:
    violations.append(
        {
            "code": code,
            "artifact": artifact,
            "message": message,
        }
    )


def arbitrate_run(
    *,
    run_root: Path,
    eval_report_path: Path,
    require_bridge: bool = True,
) -> ArbitrationResult:
    resolved_run_root = run_root.resolve()
    reports_dir = resolved_run_root / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)

    report_path = reports_dir / "arbitration_report.json"
    staged_eval_report_path = reports_dir / "eval_gate_report.for_arbitration.json"

    violations: list[dict[str, Any]] = []
    checks: list[dict[str, Any]] = []

    if not eval_report_path.exists():
        _append_violation(
            violations,
            "missing_eval_report",
            f"Eval report does not exist: {eval_report_path}",
            "eval_gate_report",
        )
    else:
        staged_eval_report_path.write_text(eval_report_path.read_text(encoding="utf-8"), encoding="utf-8")

    loaded_payloads: dict[str, dict[str, Any]] = {}

    for artifact_name, artifact_path, expected_artifact_type in _iter_required_artifacts(
        resolved_run_root,
        require_bridge,
    ):
        if not artifact_path.exists():
            _append_violation(
                violations,
                "missing_artifact",
                f"Missing required artifact: {artifact_path}",
                artifact_name,
            )
            continue

        try:
            payload = _load_json_dict(artifact_path)
        except Exception as exc:
            _append_violation(
                violations,
                "invalid_json",
                f"Invalid JSON at {artifact_path}: {exc}",
                artifact_name,
            )
            continue

        errors = validate_contract_sections(payload, expected_artifact_type=expected_artifact_type)
        for error in errors:
            _append_violation(
                violations,
                "contract_violation",
                f"{artifact_path}: {error}",
                artifact_name,
            )

        loaded_payloads[artifact_name] = payload

        checks.append(
            {
                "artifact": artifact_name,
                "exists": True,
                "contract_ok": len(errors) == 0,
            }
        )

    if staged_eval_report_path.exists():
        try:
            eval_payload = _load_json_dict(staged_eval_report_path)
            eval_errors = validate_contract_sections(eval_payload, expected_artifact_type="eval_gate_report")
            for error in eval_errors:
                _append_violation(
                    violations,
                    "contract_violation",
                    f"{staged_eval_report_path}: {error}",
                    "eval_gate_report",
                )
            loaded_payloads["eval_gate_report"] = eval_payload
            checks.append(
                {
                    "artifact": "eval_gate_report",
                    "exists": True,
                    "contract_ok": len(eval_errors) == 0,
                }
            )
        except Exception as exc:
            _append_violation(
                violations,
                "invalid_json",
                f"Invalid JSON at {staged_eval_report_path}: {exc}",
                "eval_gate_report",
            )

    # Cross-artifact path references must resolve to existing files inside run root.
    for artifact_name, payload in loaded_payloads.items():
        if artifact_name == "eval_gate_report":
            continue
        paths = payload.get("paths", {})
        if not isinstance(paths, dict):
            continue

        for key_path, relative_value in iter_path_values(paths):
            if relative_value == ".":
                continue
            try:
                resolved = _resolve_in_run_root(resolved_run_root, relative_value)
            except Exception as exc:
                _append_violation(
                    violations,
                    "path_policy_violation",
                    f"`{artifact_name}.paths.{key_path}` invalid: {exc}",
                    artifact_name,
                )
                continue

            if not resolved.exists():
                _append_violation(
                    violations,
                    "unresolved_cross_reference",
                    f"`{artifact_name}.paths.{key_path}` -> {relative_value} does not exist under run root",
                    artifact_name,
                )

    # Strict routing quarantine check.
    intent_payload = loaded_payloads.get("intent_ir")
    if intent_payload is not None:
        routing = intent_payload.get("data", {}).get("routing", {})
        quarantined_count = int(routing.get("quarantined", {}).get("count", 0)) if isinstance(routing, dict) else 0
        if quarantined_count > 0:
            _append_violation(
                violations,
                "routing_quarantine_present",
                f"Strict mode violation: {quarantined_count} routing records are quarantined",
                "intent_ir",
            )

    # Topology validity check.
    geometry_payload = loaded_payloads.get("geometry_ir")
    if geometry_payload is not None:
        features = geometry_payload.get("data", {}).get("features", [])
        if isinstance(features, list):
            invalid_count = 0
            for feature in features:
                if not isinstance(feature, dict):
                    continue
                topology = feature.get("topology", {})
                if isinstance(topology, dict) and topology.get("is_valid") is not True:
                    invalid_count += 1
            if invalid_count > 0:
                _append_violation(
                    violations,
                    "topology_invalid",
                    f"Geometry artifact contains {invalid_count} invalid features",
                    "geometry_ir",
                )

    # Deterministic hash checks from bridge manifest.
    bridge_manifest = loaded_payloads.get("bridge_manifest")
    if bridge_manifest is not None:
        manifest_paths = bridge_manifest.get("paths", {})
        manifest_data = bridge_manifest.get("data", {})
        hashes = manifest_data.get("hashes", {}) if isinstance(manifest_data, dict) else {}

        try:
            points_path = _resolve_in_run_root(resolved_run_root, manifest_paths["normalized_points_csv"])
            rule_pack_path = _resolve_in_run_root(resolved_run_root, manifest_paths["rule_pack"])
            intent_path = _resolve_in_run_root(resolved_run_root, manifest_paths["intent_ir"])
            geometry_path = _resolve_in_run_root(resolved_run_root, manifest_paths["geometry_ir"])

            known_rel_paths = _read_input_rel_paths(resolved_run_root)
            points_rows = _load_points(points_path, known_rel_paths)
            points_digest = _build_points_digest(points_rows)
            if hashes.get("points_sha256") != points_digest:
                _append_violation(
                    violations,
                    "determinism_hash_mismatch",
                    "bridge_manifest hash mismatch for points_sha256",
                    "bridge_manifest",
                )

            rule_pack_digest = _sha256_file(rule_pack_path)
            if hashes.get("rule_pack_sha256") != rule_pack_digest:
                _append_violation(
                    violations,
                    "determinism_hash_mismatch",
                    "bridge_manifest hash mismatch for rule_pack_sha256",
                    "bridge_manifest",
                )

            intent_payload_hash = _sha256_json_data(_load_json_dict(intent_path).get("data", {}))
            if hashes.get("intent_data_sha256") != intent_payload_hash:
                _append_violation(
                    violations,
                    "determinism_hash_mismatch",
                    "bridge_manifest hash mismatch for intent_data_sha256",
                    "bridge_manifest",
                )

            geometry_payload_hash = _sha256_json_data(_load_json_dict(geometry_path).get("data", {}))
            if hashes.get("geometry_data_sha256") != geometry_payload_hash:
                _append_violation(
                    violations,
                    "determinism_hash_mismatch",
                    "bridge_manifest hash mismatch for geometry_data_sha256",
                    "bridge_manifest",
                )
        except Exception as exc:
            _append_violation(
                violations,
                "determinism_hash_mismatch",
                f"Unable to run deterministic hash checks: {exc}",
                "bridge_manifest",
            )

    violations.sort(key=lambda item: (item["artifact"], item["code"], item["message"]))
    checks.sort(key=lambda item: item["artifact"])

    ok = not violations
    report_payload = build_contract_payload(
        artifact_type="arbitration_report",
        invariants=[
            build_invariant("all_required_artifacts_present", passed=all(check.get("exists", False) for check in checks)),
            build_invariant("all_contracts_valid", passed=all(check.get("contract_ok", False) for check in checks)),
            build_invariant("no_routing_quarantines_in_strict_mode", passed=not any(v["code"] == "routing_quarantine_present" for v in violations)),
            build_invariant("no_topology_invalid_features", passed=not any(v["code"] == "topology_invalid" for v in violations)),
            build_invariant("determinism_hashes_match", passed=not any(v["code"] == "determinism_hash_mismatch" for v in violations)),
        ],
        metadata={
            "ok": ok,
            "require_bridge": require_bridge,
            "violation_count": len(violations),
        },
        paths={
            "run_root": ".",
            "report": to_stable_relative_path(report_path, base=resolved_run_root),
            "eval_report": to_stable_relative_path(staged_eval_report_path, base=resolved_run_root),
        },
        data={
            "checks": checks,
            "violations": violations,
        },
    )
    write_contract_json(report_path, report_payload)

    return ArbitrationResult(ok=ok, report_path=report_path, report=report_payload)
