from __future__ import annotations

import csv
import hashlib
import json
import re
from collections import Counter
from dataclasses import asdict, dataclass, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from . import __version__
from .converter import run_converter_command
from .detection import detect_file_type, discover_files
from .json_contract import (
    build_contract_payload,
    build_invariant,
    to_stable_relative_path,
    write_contract_json,
)
from .models import (
    InputFileRecord,
    ParseResult,
    QCFinding,
    QuarantinedFile,
    RunArtifacts,
    RunSummary,
    SCHEMA_VERSION,
)
from .parsers import parse_ascii_dxf, parse_csv_file, parse_text_crd
from .qc import run_qc, summarize_findings

_SEVERITY_ORDER = {"critical": 0, "error": 1, "warning": 2, "info": 3}
_REMEDIATION_ROW_COUNT_KEYS = (
    "rows_read",
    "rows_after_remediation",
    "rows_dropped_footer",
    "rows_dropped_duplicate_tail",
)


@dataclass(slots=True)
class PipelineRunResult:
    exit_code: int
    run_id: str
    run_root: Path
    summary: RunSummary
    artifacts: RunArtifacts
    findings: list[QCFinding]
    input_files: list[InputFileRecord]


def _utc_now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat()


def _generate_run_id() -> str:
    return datetime.now(UTC).strftime("run-%Y%m%dT%H%M%SZ")


def _mk_finding(
    *,
    run_id: str,
    severity: str,
    code: str,
    message: str,
    file_path: str,
    row_number: int | None,
) -> QCFinding:
    return QCFinding(
        finding_id="",
        severity=severity,
        code=code,
        message=message,
        file_path=file_path,
        row_number=row_number,
        run_id=run_id,
    )


def _finalize_findings(run_id: str, findings: list[QCFinding]) -> list[QCFinding]:
    sorted_findings = sorted(
        findings,
        key=lambda f: (
            _SEVERITY_ORDER.get(f.severity, 99),
            f.code,
            f.file_path,
            -1 if f.row_number is None else f.row_number,
            f.message,
        )
    )
    return [
        replace(finding, finding_id=f"{run_id}-F{idx:06d}")
        for idx, finding in enumerate(sorted_findings, start=1)
    ]


def _as_sorted_dict(record: Any) -> dict[str, Any]:
    if hasattr(record, "__dataclass_fields__"):
        payload = asdict(record)
    elif isinstance(record, dict):
        payload = dict(record)
    else:
        raise TypeError(f"Unsupported record type: {type(record)}")
    return payload


def _write_csv(path: Path, records: list[Any], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for item in records:
            row = _as_sorted_dict(item)
            writer.writerow({name: row.get(name) for name in fieldnames})


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def _write_findings_jsonl(path: Path, findings: list[QCFinding]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        for finding in findings:
            payload = asdict(finding)
            payload["schema_version"] = SCHEMA_VERSION
            handle.write(json.dumps(payload, sort_keys=True))
            handle.write("\n")


def _sanitize_namespace(namespace: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "-", namespace.strip()).strip("-")
    return cleaned[:64] or "default"


def _sha256_file(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def _relative_snapshot_path(input_dir: Path, file_path: Path) -> str:
    try:
        return file_path.resolve().relative_to(input_dir.resolve()).as_posix()
    except ValueError:
        return to_stable_relative_path(file_path, base=input_dir)


def _run_relative_path(run_root: Path, path: Path) -> str:
    return path.resolve().relative_to(run_root.resolve()).as_posix()


def _build_dataset_snapshot(input_dir: Path, files: list[Path]) -> dict[str, Any]:
    records: list[dict[str, Any]] = []
    for file_path in sorted(files, key=lambda item: item.as_posix()):
        records.append(
            {
                "relative_path": _relative_snapshot_path(input_dir, file_path),
                "size_bytes": file_path.stat().st_size,
                "sha256": _sha256_file(file_path),
            }
        )

    fingerprint = hashlib.sha256()
    for record in records:
        fingerprint.update(record["relative_path"].encode("utf-8"))
        fingerprint.update(b"\0")
        fingerprint.update(str(record["size_bytes"]).encode("utf-8"))
        fingerprint.update(b"\0")
        fingerprint.update(record["sha256"].encode("utf-8"))
        fingerprint.update(b"\0")

    return build_contract_payload(
        artifact_type="dataset_snapshot",
        invariants=[
            build_invariant("snapshot_fingerprint_is_sha256"),
        ],
        metadata={
            "input_root": ".",
        },
        paths={
            "input_root": ".",
        },
        data={
            "snapshot_id": f"sha256:{fingerprint.hexdigest()}",
            "snapshot_algorithm": "sha256",
            "file_count": len(records),
            "files": records,
        },
    )


def _read_json_dict(path: Path) -> dict[str, Any] | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    if not isinstance(payload, dict):
        return None
    return payload


def _manifest_summary_dict(payload: dict[str, Any]) -> dict[str, Any] | None:
    data = payload.get("data")
    if isinstance(data, dict):
        summary = data.get("summary")
        if isinstance(summary, dict):
            return summary
    summary = payload.get("summary")
    if isinstance(summary, dict):
        return summary
    return None


def _manifest_run_id(payload: dict[str, Any]) -> str | None:
    metadata = payload.get("metadata")
    if isinstance(metadata, dict):
        run_id = metadata.get("run_id")
        if isinstance(run_id, str):
            return run_id
    run_id = payload.get("run_id")
    if isinstance(run_id, str):
        return run_id
    return None


def _derive_baseline_namespace(config: dict[str, Any], input_dir: Path) -> str:
    project_cfg = config.get("project", {})
    configured_namespace = project_cfg.get("baseline_namespace") if isinstance(project_cfg, dict) else None
    if isinstance(configured_namespace, str) and configured_namespace.strip():
        return _sanitize_namespace(configured_namespace)

    namespace_seed = {
        "input_dir": str(input_dir.resolve()),
        "qc_profile": (project_cfg.get("qc_profile") if isinstance(project_cfg, dict) else "") or "",
        "include_globs": sorted(config.get("input", {}).get("include_globs", [])),
        "exclude_globs": sorted(config.get("input", {}).get("exclude_globs", [])),
    }
    digest = hashlib.sha256(json.dumps(namespace_seed, sort_keys=True).encode("utf-8")).hexdigest()[:12]
    profile = str(namespace_seed["qc_profile"] or "project")
    return _sanitize_namespace(f"{profile}-{digest}")


def _load_baseline_manifest(path: Path) -> dict[str, Any] | None:
    payload = _read_json_dict(path)
    if payload is None:
        return None
    summary = _manifest_summary_dict(payload)
    if not isinstance(summary, dict):
        return None
    findings_by_severity = summary.get("findings_by_severity")
    if not isinstance(findings_by_severity, dict):
        return None
    normalized_counts: dict[str, int] = {}
    for key, value in findings_by_severity.items():
        if isinstance(key, str) and isinstance(value, int):
            normalized_counts[key] = value
    return {
        "run_id": _manifest_run_id(payload),
        "findings_by_severity": normalized_counts,
        "manifest_path": path.as_posix(),
    }


def _resolve_trend_anchor_dir(output_dir: Path, config_anchor_dir: Path | None) -> Path:
    return config_anchor_dir.resolve() if config_anchor_dir is not None else output_dir.resolve()


def _resolve_trend_state_path(
    output_dir: Path,
    trend_cfg: dict[str, Any],
    namespace: str,
    config_anchor_dir: Path | None,
) -> Path:
    configured_state = trend_cfg.get("state_file_path")
    if isinstance(configured_state, str) and configured_state.strip():
        rendered = configured_state.strip().replace("{namespace}", namespace)
        state_path = Path(rendered)
        if not state_path.is_absolute():
            anchor_dir = _resolve_trend_anchor_dir(output_dir, config_anchor_dir)
            state_path = (anchor_dir / state_path).resolve()
        return state_path
    return output_dir / "trend_baselines" / namespace / "last_good_run.json"


def _resolve_qc_baseline(
    output_dir: Path,
    trend_cfg: dict[str, Any],
    namespace: str,
    config_anchor_dir: Path | None,
) -> dict[str, Any] | None:
    configured_manifest = trend_cfg.get("baseline_manifest_path")
    if isinstance(configured_manifest, str) and configured_manifest.strip():
        manifest_path = Path(configured_manifest.strip())
        if not manifest_path.is_absolute():
            anchor_dir = _resolve_trend_anchor_dir(output_dir, config_anchor_dir)
            manifest_path = (anchor_dir / manifest_path).resolve()
        return _load_baseline_manifest(manifest_path)

    state_path = _resolve_trend_state_path(
        output_dir=output_dir,
        trend_cfg=trend_cfg,
        namespace=namespace,
        config_anchor_dir=config_anchor_dir,
    )
    state_payload = _read_json_dict(state_path)
    if state_payload is None:
        return None
    state_metadata = state_payload.get("metadata", {})
    state_namespace = state_metadata.get("namespace") if isinstance(state_metadata, dict) else state_payload.get("namespace")
    if not isinstance(state_namespace, str) or state_namespace != namespace:
        return None
    state_paths = state_payload.get("paths", {})
    if not isinstance(state_paths, dict):
        legacy_manifest_path = state_payload.get("manifest_path")
        if isinstance(legacy_manifest_path, str) and legacy_manifest_path.strip():
            return _load_baseline_manifest(Path(legacy_manifest_path).resolve())
        return None
    run_manifest_rel = state_paths.get("run_manifest")
    if not isinstance(run_manifest_rel, str) or not run_manifest_rel.strip():
        return None
    run_id = state_metadata.get("run_id") if isinstance(state_metadata, dict) else state_payload.get("run_id")
    if not isinstance(run_id, str) or not run_id.strip():
        return None
    run_root = output_dir / run_id
    manifest_path = (run_root / run_manifest_rel).resolve()
    return _load_baseline_manifest(manifest_path)


def _build_qc_trend_payload(
    *,
    run_id: str,
    namespace: str,
    current_counts: dict[str, int],
    trend_cfg: dict[str, Any],
    baseline: dict[str, Any] | None,
) -> dict[str, Any]:
    baseline_summary: dict[str, Any] | None = None
    if baseline is not None:
        baseline_counts = baseline.get("findings_by_severity", {})
        if isinstance(baseline_counts, dict):
            baseline_summary = {
                "run_id": baseline.get("run_id"),
                "findings_by_severity": dict(sorted(baseline_counts.items())),
            }

    payload = {
        "run_id": run_id,
        "namespace": namespace,
        "enabled": bool(trend_cfg.get("enabled", False)),
        "baseline": baseline_summary,
        "current_findings_by_severity": dict(sorted(current_counts.items())),
        "deltas": {},
        "thresholds": {
            "max_warning_delta": trend_cfg.get("max_warning_delta"),
            "max_error_delta": trend_cfg.get("max_error_delta"),
            "max_critical_delta": trend_cfg.get("max_critical_delta"),
        },
        "spikes": [],
        "spike_detected": False,
        "comparison_available": baseline_summary is not None,
    }

    if baseline_summary is None:
        return payload

    baseline_counts = baseline_summary.get("findings_by_severity")
    if not isinstance(baseline_counts, dict):
        return payload

    deltas: dict[str, int] = {}
    spikes: list[dict[str, Any]] = []
    for severity, threshold_key in [
        ("warning", "max_warning_delta"),
        ("error", "max_error_delta"),
        ("critical", "max_critical_delta"),
    ]:
        current = int(current_counts.get(severity, 0))
        baseline_count = int(baseline_counts.get(severity, 0))
        delta = current - baseline_count
        deltas[severity] = delta
        threshold = trend_cfg.get(threshold_key)
        if isinstance(threshold, int) and delta > threshold:
            spikes.append(
                {
                    "severity": severity,
                    "delta": delta,
                    "baseline_count": baseline_count,
                    "current_count": current,
                    "threshold": threshold,
                }
            )

    payload["deltas"] = deltas
    payload["spikes"] = spikes
    payload["spike_detected"] = bool(spikes)
    return payload


def _write_last_good_state(
    *,
    state_path: Path,
    namespace: str,
    run_id: str,
    run_manifest_path: str,
    qc_summary_path: str,
    findings_by_severity: dict[str, int],
) -> None:
    state_payload = build_contract_payload(
        artifact_type="trend_baseline_state",
        invariants=[
            build_invariant("state_targets_run_root_relative_artifacts"),
        ],
        metadata={
            "namespace": namespace,
            "run_id": run_id,
            "recorded_at": _utc_now_iso(),
        },
        paths={
            "run_manifest": run_manifest_path,
            "qc_summary": qc_summary_path,
        },
        data={
            "findings_by_severity": dict(sorted(findings_by_severity.items())),
        },
    )
    write_contract_json(state_path, state_payload)


def _build_presentation_palette(config: dict[str, Any]) -> dict[str, Any]:
    presentation_cfg = config.get("presentation", {})
    category_colors = presentation_cfg.get("category_colors", {})
    config_colors = presentation_cfg.get("config_colors", {})
    qc_profile_colors = config_colors.get("qc_profile", {}) if isinstance(config_colors, dict) else {}
    crd_mode_colors = config_colors.get("crd_mode", {}) if isinstance(config_colors, dict) else {}
    return {
        "enabled": bool(presentation_cfg.get("enabled", True)),
        "color_basis": presentation_cfg.get("color_basis", "category_config"),
        "category_colors": dict(category_colors) if isinstance(category_colors, dict) else {},
        "config_colors": {
            "qc_profile": dict(qc_profile_colors) if isinstance(qc_profile_colors, dict) else {},
            "crd_mode": dict(crd_mode_colors) if isinstance(crd_mode_colors, dict) else {},
        },
    }


def _build_phase_presentation(
    *,
    config: dict[str, Any],
    dataset_snapshot_id: str,
    files_total: int,
    files_processed: int,
    files_quarantined: int,
    exit_code: int,
    findings_by_severity: dict[str, int],
) -> dict[str, Any]:
    palette = _build_presentation_palette(config)
    project_cfg = config.get("project", {})
    crd_cfg = config.get("crd", {})
    qc_profile = project_cfg.get("qc_profile")
    crd_mode = crd_cfg.get("mode")

    if files_processed == 0:
        phase_2_status = "fail"
    elif files_quarantined > 0:
        phase_2_status = "warning"
    else:
        phase_2_status = "pass"

    if exit_code == 0:
        phase_3_status = "pass"
    elif exit_code == 2:
        phase_3_status = "warning"
    else:
        phase_3_status = "fail"

    return {
        "enabled": palette["enabled"],
        "color_basis": palette["color_basis"],
        "palette": palette,
        "ground_truth": {
            "meaning": "baseline dataset snapshot and config context for this run",
            "evidence": {
                "snapshot_id": dataset_snapshot_id,
                "qc_profile": qc_profile,
                "crd_mode": crd_mode,
            },
            "config_colors": {
                "qc_profile": palette["config_colors"]["qc_profile"].get(str(qc_profile)),
                "crd_mode": palette["config_colors"]["crd_mode"].get(str(crd_mode)),
            },
        },
        "phase_1": {
            "meaning": "input and config readiness",
            "status": "pass" if files_total > 0 else "fail",
            "evidence": {
                "files_total": files_total,
                "config_valid": True,
            },
        },
        "phase_2": {
            "meaning": "normalization and conversion execution",
            "status": phase_2_status,
            "evidence": {
                "files_processed": files_processed,
                "files_quarantined": files_quarantined,
            },
        },
        "phase_3": {
            "meaning": "qc, reporting, and release artifact completion",
            "status": phase_3_status,
            "evidence": {
                "exit_code": exit_code,
                "findings_by_severity": dict(sorted(findings_by_severity.items())),
            },
        },
    }


def _write_points_parquet(path: Path, points: list[Any]) -> tuple[bool, str]:
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq

        columns = [
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

        if points:
            table = pa.Table.from_pylist([asdict(item) for item in points])
        else:
            schema = pa.schema(
                [
                    ("point_id", pa.string()),
                    ("northing", pa.float64()),
                    ("easting", pa.float64()),
                    ("elevation", pa.float64()),
                    ("description", pa.string()),
                    ("dwg_description", pa.string()),
                    ("dwg_layer", pa.string()),
                    ("locked", pa.string()),
                    ("group_name", pa.string()),
                    ("category", pa.string()),
                    ("ls_number", pa.string()),
                    ("source_file", pa.string()),
                    ("source_line", pa.int64()),
                ]
            )
            arrays = [pa.array([], type=field.type) for field in schema]
            table = pa.Table.from_arrays(arrays, schema=schema)

        table = table.select(columns)
        pq.write_table(table, path)
        return True, ""
    except Exception as exc:
        # Cleanup potentially partial file if it exists
        if path.exists():
            try:
                path.unlink()
            except Exception:
                pass
        return False, str(exc)


def profile_input(input_dir: Path, config: dict) -> dict[str, Any]:
    files = discover_files(
        input_dir=input_dir,
        include_globs=config["input"]["include_globs"],
        exclude_globs=config["input"]["exclude_globs"],
    )

    profiles: list[dict[str, Any]] = []
    type_counter: Counter[str] = Counter()
    for file_path in files:
        detected_type, message = detect_file_type(file_path)
        type_counter[detected_type] += 1
        profiles.append(
            {
                "file_path": str(file_path),
                "detected_type": detected_type,
                "size_bytes": file_path.stat().st_size,
                "message": message,
            }
        )

    return {
        "schema_version": SCHEMA_VERSION,
        "input_dir": str(input_dir),
        "files_total": len(files),
        "files_by_type": dict(sorted(type_counter.items())),
        "files": profiles,
    }


def run_pipeline(
    *,
    input_dir: Path,
    output_dir: Path,
    config: dict,
    run_id: str | None,
    config_anchor_dir: Path | None = None,
) -> PipelineRunResult:
    started_at = _utc_now_iso()
    effective_run_id = run_id or _generate_run_id()
    run_root = output_dir / effective_run_id

    normalized_dir = run_root / "normalized"
    reports_dir = run_root / "reports"
    quarantine_dir = run_root / "quarantine"
    manifest_dir = run_root / "manifest"
    temp_dir = run_root / "_tmp"

    for directory in [normalized_dir, reports_dir, quarantine_dir, manifest_dir, temp_dir]:
        directory.mkdir(parents=True, exist_ok=True)

    files = discover_files(
        input_dir=input_dir,
        include_globs=config["input"]["include_globs"],
        exclude_globs=config["input"]["exclude_globs"],
    )

    dataset_snapshot_json = manifest_dir / "dataset_snapshot.json"
    dataset_snapshot_payload = _build_dataset_snapshot(input_dir, files)
    write_contract_json(dataset_snapshot_json, dataset_snapshot_payload)

    points = []
    field_code_rules = []
    dxf_entities = []
    quarantined_rows = []
    quarantined_files: list[QuarantinedFile] = []
    findings: list[QCFinding] = []
    input_file_records: list[InputFileRecord] = []
    remediation_row_counts_by_file: list[dict[str, Any]] = []

    processed_files = 0

    crd_mode = config["crd"]["mode"]
    converter_command = config["crd"].get("converter_command")
    converter_failure_mode = config["crd"].get("converter_failure_mode", "fatal")
    fail_on_converter_error = converter_failure_mode != "quarantine"

    for file_path in files:
        detected_type, detection_message = detect_file_type(file_path)
        record = InputFileRecord(
            file_path=str(file_path),
            detected_type=detected_type,
            size_bytes=file_path.stat().st_size,
            status="pending",
            message=detection_message,
        )

        try:
            parse_result = ParseResult()

            if detected_type in {"point_csv", "field_code_csv", "mixed_csv"}:
                parse_result = parse_csv_file(file_path, detected_type, config, effective_run_id)
                record.status = "processed_with_warnings" if parse_result.quarantined_rows else "processed"
                processed_files += 1

            elif detected_type == "ascii_dxf":
                parse_result = parse_ascii_dxf(file_path, effective_run_id)
                record.status = "processed"
                processed_files += 1

            elif detected_type == "binary_dxf":
                record.status = "quarantined"
                quarantined_files.append(
                    QuarantinedFile(
                        file_path=str(file_path),
                        reason="unsupported_binary_dxf",
                        message="Binary DXF is out of v1 scope",
                    )
                )
                findings.append(
                    _mk_finding(
                        run_id=effective_run_id,
                        severity="warning",
                        code="unsupported_binary_dxf",
                        message="Binary DXF encountered and quarantined",
                        file_path=str(file_path),
                        row_number=None,
                    )
                )

            elif detected_type == "crd_text":
                parse_result = parse_text_crd(file_path, effective_run_id)
                record.status = "processed_with_warnings" if parse_result.quarantined_rows else "processed"
                processed_files += 1

            elif detected_type == "crd_binary":
                if crd_mode == "text_only":
                    record.status = "quarantined"
                    quarantined_files.append(
                        QuarantinedFile(
                            file_path=str(file_path),
                            reason="binary_crd_text_only_mode",
                            message="Binary CRD deferred in text_only mode",
                        )
                    )
                    findings.append(
                        _mk_finding(
                            run_id=effective_run_id,
                            severity="warning",
                            code="binary_crd_text_only_mode",
                            message="Binary CRD quarantined because crd.mode=text_only",
                            file_path=str(file_path),
                            row_number=None,
                        )
                    )
                else:
                    if not converter_command:
                        if crd_mode == "converter_required" and fail_on_converter_error:
                            raise RuntimeError("Binary CRD requires  in converter_required mode")
                        record.status = "quarantined"
                        quarantined_files.append(
                            QuarantinedFile(
                                file_path=str(file_path),
                                reason="binary_crd_converter_missing",
                                message="No converter command configured for binary CRD",
                            )
                        )
                        findings.append(
                            _mk_finding(
                                run_id=effective_run_id,
                                severity="warning",
                                code="binary_crd_converter_missing",
                                message="Binary CRD quarantined because converter is not configured",
                                file_path=str(file_path),
                                row_number=None,
                            )
                        )
                    else:
                        converted_path = temp_dir / f"{file_path.stem}.converted.csv"
                        ok, converter_message = run_converter_command(
                            converter_command,
                            file_path,
                            converted_path,
                        )
                        if not ok:
                            if crd_mode == "converter_required" and fail_on_converter_error:
                                raise RuntimeError(f"CRD converter failed: {converter_message}")
                            record.status = "quarantined"
                            quarantined_files.append(
                                QuarantinedFile(
                                    file_path=str(file_path),
                                    reason="binary_crd_converter_failed",
                                    message=converter_message,
                                )
                            )
                            findings.append(
                                _mk_finding(
                                    run_id=effective_run_id,
                                    severity="warning",
                                    code="binary_crd_converter_failed",
                                    message=f"Binary CRD converter failed: {converter_message}",
                                    file_path=str(file_path),
                                    row_number=None,
                                )
                            )
                        else:
                            converted_type, converted_message = detect_file_type(converted_path)
                            if converted_type in {"point_csv", "field_code_csv"}:
                                parse_result = parse_csv_file(converted_path, converted_type, config, effective_run_id)
                            elif converted_type == "crd_text":
                                parse_result = parse_text_crd(converted_path, effective_run_id)
                            else:
                                unsupported_message = (
                                    "Converted CRD output is unsupported for downstream parsing: "
                                    f"{converted_type} ({converted_message})"
                                )
                                if crd_mode == "converter_required" and fail_on_converter_error:
                                    raise RuntimeError(unsupported_message)
                                record.status = "quarantined"
                                quarantined_files.append(
                                    QuarantinedFile(
                                        file_path=str(file_path),
                                        reason="binary_crd_converted_output_unsupported",
                                        message=unsupported_message,
                                    )
                                )
                                findings.append(
                                    _mk_finding(
                                        run_id=effective_run_id,
                                        severity="warning",
                                        code="binary_crd_converted_output_unsupported",
                                        message=unsupported_message,
                                        file_path=str(file_path),
                                        row_number=None,
                                    )
                                )

                            if record.status != "quarantined":
                                record.status = (
                                    "processed_with_warnings" if parse_result.quarantined_rows else "processed"
                                )
                                processed_files += 1

            else:
                record.status = "quarantined"
                quarantined_files.append(
                    QuarantinedFile(
                        file_path=str(file_path),
                        reason="unsupported_file_type",
                        message=f"Unsupported file type for v1: {detected_type}",
                    )
                )
                findings.append(
                    _mk_finding(
                        run_id=effective_run_id,
                        severity="warning",
                        code="unsupported_file_type",
                        message=f"File type {detected_type} is currently unsupported",
                        file_path=str(file_path),
                        row_number=None,
                    )
                )

            points.extend(parse_result.points)
            field_code_rules.extend(parse_result.field_code_rules)
            dxf_entities.extend(parse_result.dxf_entities)
            quarantined_rows.extend(parse_result.quarantined_rows)
            findings.extend(parse_result.findings)
            if parse_result.remediation_row_counts:
                row_counts = {
                    "file_path": _relative_snapshot_path(input_dir, file_path),
                }
                for key in _REMEDIATION_ROW_COUNT_KEYS:
                    row_counts[key] = int(parse_result.remediation_row_counts.get(key, 0))
                remediation_row_counts_by_file.append(row_counts)

        except Exception as exc:
            record.status = "quarantined"
            quarantined_files.append(
                QuarantinedFile(
                    file_path=str(file_path),
                    reason="processing_error",
                    message=str(exc),
                )
            )
            findings.append(
                _mk_finding(
                    run_id=effective_run_id,
                    severity="error",
                    code="processing_error",
                    message=f"Failed to process file: {exc}",
                    file_path=str(file_path),
                    row_number=None,
                )
            )

        input_file_records.append(record)

    points.sort(key=lambda p: (p.source_file, p.source_line, p.point_id))
    field_code_rules.sort(key=lambda r: (r.source_file, r.source_line, r.field_code))
    dxf_entities.sort(
        key=lambda e: (
            e.source_file,
            e.handle,
            e.entity_type,
            e.layer,
            float("inf") if e.x is None else e.x,
            float("inf") if e.y is None else e.y,
            float("inf") if e.z is None else e.z,
        )
    )
    quarantined_rows.sort(key=lambda q: (q.file_path, q.row_number, q.reason))
    quarantined_files.sort(key=lambda q: (q.file_path, q.reason, q.message))
    input_file_records.sort(key=lambda r: r.file_path)
    remediation_row_counts_by_file.sort(key=lambda item: item["file_path"])

    findings.extend(run_qc(points, field_code_rules, config, effective_run_id))

    fail_if_all_invalid = bool(config["validation"]["fail_if_all_files_invalid"])
    fatal_issue = False
    if fail_if_all_invalid and processed_files == 0:
        fatal_issue = True
        findings.append(
            _mk_finding(
                run_id=effective_run_id,
                severity="critical",
                code="all_files_invalid",
                message="No files were successfully processed",
                file_path="",
                row_number=None,
            )
        )

    points_csv = normalized_dir / "points.csv"
    points_parquet = normalized_dir / "points.parquet"
    field_rules_csv = normalized_dir / "field_code_rules.csv"
    dxf_entities_csv = normalized_dir / "dxf_entities.csv"
    findings_jsonl = reports_dir / "qc_findings.jsonl"
    qc_summary_json = reports_dir / "qc_summary.json"
    qc_trend_json = reports_dir / "qc_trend.json"
    quarantined_rows_csv = quarantine_dir / "quarantined_rows.csv"
    quarantined_files_json = quarantine_dir / "quarantined_files.json"
    run_manifest_json = manifest_dir / "run_manifest.json"

    _write_csv(
        points_csv,
        points,
        [
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
        ],
    )
    _write_csv(
        field_rules_csv,
        field_code_rules,
        ["field_code", "layer", "symbol", "linework", "source_file", "source_line"],
    )
    _write_csv(
        dxf_entities_csv,
        dxf_entities,
        ["entity_type", "layer", "x", "y", "z", "text", "handle", "source_file"],
    )
    _write_csv(
        quarantined_rows_csv,
        quarantined_rows,
        ["file_path", "row_number", "reason", "raw_row", "details"],
    )

    parquet_ok = True
    parquet_message = ""
    if "parquet" in config["outputs"]["formats"]:
        parquet_ok, parquet_message = _write_points_parquet(points_parquet, points)
        if not parquet_ok:
            findings.append(
                _mk_finding(
                    run_id=effective_run_id,
                    severity="warning",
                    code="parquet_write_failed",
                    message=f"Failed to write parquet output: {parquet_message}",
                    file_path=str(points_parquet),
                    row_number=None,
                )
            )
    findings_by_severity_before_trend = summarize_findings(findings)
    files_by_type = dict(sorted(Counter(record.detected_type for record in input_file_records).items()))

    warning_threshold = config["validation"].get("max_warning_count")
    warning_count_before_trend = findings_by_severity_before_trend.get("warning", 0)
    warning_threshold_exceeded = warning_threshold is not None and warning_count_before_trend > warning_threshold

    trend_cfg = config["validation"].get("trend_tracking", {})
    trend_enabled = bool(trend_cfg.get("enabled", False))
    trend_namespace = _derive_baseline_namespace(config, input_dir)
    baseline = (
        _resolve_qc_baseline(
            output_dir=output_dir,
            trend_cfg=trend_cfg,
            namespace=trend_namespace,
            config_anchor_dir=config_anchor_dir,
        )
        if trend_enabled
        else None
    )
    qc_trend_payload = _build_qc_trend_payload(
        run_id=effective_run_id,
        namespace=trend_namespace,
        current_counts=findings_by_severity_before_trend,
        trend_cfg=trend_cfg,
        baseline=baseline,
    )
    if trend_enabled and bool(trend_cfg.get("fail_on_spike", True)) and qc_trend_payload.get("spike_detected"):
        fatal_issue = True
        spike_summary = ", ".join(
            f"{item['severity']} delta={item['delta']} (>{item['threshold']})"
            for item in qc_trend_payload.get("spikes", [])
        )
        findings.append(
            _mk_finding(
                run_id=effective_run_id,
                severity="critical",
                code="qc_regression_spike",
                message=f"QC regression spike detected against baseline: {spike_summary}",
                file_path="",
                row_number=None,
            )
        )

    findings = _finalize_findings(effective_run_id, findings)
    _write_findings_jsonl(findings_jsonl, findings)

    findings_by_severity = summarize_findings(findings)
    qc_trend_payload["final_findings_by_severity"] = dict(sorted(findings_by_severity.items()))
    qc_trend_contract_payload = build_contract_payload(
        artifact_type="qc_trend",
        invariants=[
            build_invariant("trend_delta_computation_stable"),
        ],
        metadata={
            "run_id": effective_run_id,
            "namespace": trend_namespace,
            "enabled": trend_enabled,
        },
        paths={
            "qc_summary": _run_relative_path(run_root, qc_summary_json),
            "run_manifest": _run_relative_path(run_root, run_manifest_json),
        },
        data=qc_trend_payload,
    )
    write_contract_json(qc_trend_json, qc_trend_contract_payload)

    has_warnings = bool(findings) or bool(quarantined_files) or bool(quarantined_rows)
    if fatal_issue:
        exit_code = 3
    elif has_warnings:
        exit_code = 2
    else:
        exit_code = 0

    phase_presentation = _build_phase_presentation(
        config=config,
        dataset_snapshot_id=dataset_snapshot_payload["data"]["snapshot_id"],
        files_total=len(input_file_records),
        files_processed=processed_files,
        files_quarantined=len(quarantined_files),
        exit_code=exit_code,
        findings_by_severity=findings_by_severity,
    )

    remediation_totals = {key: 0 for key in _REMEDIATION_ROW_COUNT_KEYS}
    for row_counts in remediation_row_counts_by_file:
        for key in _REMEDIATION_ROW_COUNT_KEYS:
            remediation_totals[key] += int(row_counts[key])
    remediation_invariant_satisfied = remediation_totals["rows_read"] == (
        remediation_totals["rows_after_remediation"]
        + remediation_totals["rows_dropped_footer"]
        + remediation_totals["rows_dropped_duplicate_tail"]
    )

    qc_summary_payload = build_contract_payload(
        artifact_type="qc_summary",
        invariants=[
            build_invariant(
                "remediation_row_count_invariant",
                passed=remediation_invariant_satisfied,
                detail=(
                    "rows_read == rows_after_remediation + rows_dropped_footer + rows_dropped_duplicate_tail"
                    if remediation_invariant_satisfied
                    else "Remediation row-count invariant failed"
                ),
            ),
        ],
        metadata={
            "run_id": effective_run_id,
            "qc_profile": config.get("project", {}).get("qc_profile"),
            "trend_namespace": trend_namespace,
        },
        paths={
            "run_root": ".",
            "run_manifest": _run_relative_path(run_root, run_manifest_json),
            "qc_findings": _run_relative_path(run_root, findings_jsonl),
            "quarantined_rows": _run_relative_path(run_root, quarantined_rows_csv),
            "quarantined_files": _run_relative_path(run_root, quarantined_files_json),
        },
        data={
            "files_by_type": files_by_type,
            "findings_by_severity": findings_by_severity,
            "trend_spike_detected": bool(qc_trend_payload.get("spike_detected", False)),
            "trend_comparison_available": bool(qc_trend_payload.get("comparison_available", False)),
            "quarantined_row_count": len(quarantined_rows),
            "quarantined_file_count": len(quarantined_files),
            "point_count": len(points),
            "field_code_rule_count": len(field_code_rules),
            "dxf_entity_count": len(dxf_entities),
            "remediation_row_counts": {
                "by_file": remediation_row_counts_by_file,
                "totals": remediation_totals,
                "invariant_satisfied": remediation_invariant_satisfied,
            },
            "phase_presentation": phase_presentation,
        },
    )
    write_contract_json(qc_summary_json, qc_summary_payload)

    quarantined_files_payload = build_contract_payload(
        artifact_type="quarantined_files",
        metadata={
            "run_id": effective_run_id,
            "quarantined_file_count": len(quarantined_files),
        },
        paths={
            "run_root": ".",
            "run_manifest": _run_relative_path(run_root, run_manifest_json),
        },
        data={
            "files": [asdict(item) for item in quarantined_files],
        },
    )
    write_contract_json(quarantined_files_json, quarantined_files_payload)

    ended_at = _utc_now_iso()

    summary = RunSummary(
        run_id=effective_run_id,
        started_at=started_at,
        ended_at=ended_at,
        files_total=len(input_file_records),
        files_processed=processed_files,
        files_quarantined=len(quarantined_files),
        findings_by_severity=findings_by_severity,
    )
    manifest_artifact_paths = {
        "run_manifest_json": _run_relative_path(run_root, run_manifest_json),
        "normalized_points_csv": _run_relative_path(run_root, points_csv),
        "normalized_points_parquet": _run_relative_path(run_root, points_parquet),
        "normalized_field_code_rules_csv": _run_relative_path(run_root, field_rules_csv),
        "normalized_dxf_entities_csv": _run_relative_path(run_root, dxf_entities_csv),
        "qc_findings_jsonl": _run_relative_path(run_root, findings_jsonl),
        "qc_summary_json": _run_relative_path(run_root, qc_summary_json),
        "qc_trend_json": _run_relative_path(run_root, qc_trend_json),
        "quarantined_rows_csv": _run_relative_path(run_root, quarantined_rows_csv),
        "quarantined_files_json": _run_relative_path(run_root, quarantined_files_json),
        "dataset_snapshot_json": _run_relative_path(run_root, dataset_snapshot_json),
    }
    manifest_input_files = [
        {
            "file_path": _relative_snapshot_path(input_dir, Path(record.file_path)),
            "detected_type": record.detected_type,
            "size_bytes": record.size_bytes,
            "status": record.status,
            "message": record.message,
        }
        for record in input_file_records
    ]
    manifest_payload = build_contract_payload(
        artifact_type="run_manifest",
        invariants=[
            build_invariant(
                "remediation_row_count_invariant",
                passed=remediation_invariant_satisfied,
                detail=(
                    "rows_read == rows_after_remediation + rows_dropped_footer + rows_dropped_duplicate_tail"
                    if remediation_invariant_satisfied
                    else "Remediation row-count invariant failed"
                ),
            ),
        ],
        metadata={
            "tool_version": __version__,
            "run_id": effective_run_id,
            "started_at": started_at,
            "ended_at": ended_at,
            "qc_profile": config.get("project", {}).get("qc_profile"),
        },
        paths={
            "run_root": ".",
            **manifest_artifact_paths,
        },
        data={
            "warning_threshold": warning_threshold,
            "warning_threshold_exceeded": warning_threshold_exceeded,
            "trend_tracking": {
                "enabled": trend_enabled,
                "namespace": trend_namespace,
                "comparison_available": bool(qc_trend_payload.get("comparison_available", False)),
                "spike_detected": bool(qc_trend_payload.get("spike_detected", False)),
            },
            "dataset_snapshot": {
                "snapshot_id": dataset_snapshot_payload["data"]["snapshot_id"],
                "file_count": dataset_snapshot_payload["data"]["file_count"],
                "snapshot_artifact": manifest_artifact_paths["dataset_snapshot_json"],
            },
            "config": {
                "version": config["version"],
                "crd_mode": config["crd"]["mode"],
                "outputs": config["outputs"]["formats"],
            },
            "phase_presentation": phase_presentation,
            "input_files": manifest_input_files,
            "summary": asdict(summary),
            "artifacts": manifest_artifact_paths,
            "remediation_row_counts": {
                "by_file": remediation_row_counts_by_file,
                "totals": remediation_totals,
                "invariant_satisfied": remediation_invariant_satisfied,
            },
        },
    )
    write_contract_json(run_manifest_json, manifest_payload)

    good_for_baseline = (
        exit_code in {0, 2}
        and findings_by_severity.get("critical", 0) == 0
        and findings_by_severity.get("error", 0) == 0
        and not warning_threshold_exceeded
        and not bool(qc_trend_payload.get("spike_detected", False))
    )
    if trend_enabled and good_for_baseline:
        _write_last_good_state(
            state_path=_resolve_trend_state_path(
                output_dir=output_dir,
                trend_cfg=trend_cfg,
                namespace=trend_namespace,
                config_anchor_dir=config_anchor_dir,
            ),
            namespace=trend_namespace,
            run_id=effective_run_id,
            run_manifest_path=manifest_artifact_paths["run_manifest_json"],
            qc_summary_path=manifest_artifact_paths["qc_summary_json"],
            findings_by_severity=findings_by_severity,
        )

    artifacts = RunArtifacts(
        normalized_points_csv=str(points_csv),
        normalized_points_parquet=str(points_parquet),
        normalized_field_code_rules_csv=str(field_rules_csv),
        normalized_dxf_entities_csv=str(dxf_entities_csv),
        qc_findings_jsonl=str(findings_jsonl),
        qc_summary_json=str(qc_summary_json),
        qc_trend_json=str(qc_trend_json),
        quarantined_rows_csv=str(quarantined_rows_csv),
        quarantined_files_json=str(quarantined_files_json),
        dataset_snapshot_json=str(dataset_snapshot_json),
        run_manifest_json=str(run_manifest_json),
    )

    return PipelineRunResult(
        exit_code=exit_code,
        run_id=effective_run_id,
        run_root=run_root,
        summary=summary,
        artifacts=artifacts,
        findings=findings,
        input_files=input_file_records,
    )
