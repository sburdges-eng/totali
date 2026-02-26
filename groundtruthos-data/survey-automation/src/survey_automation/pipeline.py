from __future__ import annotations

import csv
import json
from collections import Counter
from dataclasses import asdict, dataclass, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from . import __version__
from .converter import run_converter_command
from .detection import detect_file_type, discover_files
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
    except Exception as exc:  # pragma: no cover - exercised by environment differences
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

    points = []
    field_code_rules = []
    dxf_entities = []
    quarantined_rows = []
    quarantined_files: list[QuarantinedFile] = []
    findings: list[QCFinding] = []
    input_file_records: list[InputFileRecord] = []

    processed_files = 0

    crd_mode = config["crd"]["mode"]
    converter_command = config["crd"].get("converter_command")

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
                        if crd_mode == "converter_required":
                            raise RuntimeError("Binary CRD requires `crd.converter_command` in converter_required mode")
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
                            if crd_mode == "converter_required":
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
                                if crd_mode == "converter_required":
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

    findings = _finalize_findings(effective_run_id, findings)

    points_csv = normalized_dir / "points.csv"
    points_parquet = normalized_dir / "points.parquet"
    field_rules_csv = normalized_dir / "field_code_rules.csv"
    dxf_entities_csv = normalized_dir / "dxf_entities.csv"
    findings_jsonl = reports_dir / "qc_findings.jsonl"
    qc_summary_json = reports_dir / "qc_summary.json"
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
            findings = _finalize_findings(effective_run_id, findings)

    _write_findings_jsonl(findings_jsonl, findings)

    findings_by_severity = summarize_findings(findings)
    files_by_type = dict(sorted(Counter(record.detected_type for record in input_file_records).items()))
    warning_threshold = config["validation"].get("max_warning_count")
    warning_count = findings_by_severity.get("warning", 0)
    warning_threshold_exceeded = (
        warning_threshold is not None and warning_count > warning_threshold
    )

    qc_summary_payload = {
        "schema_version": SCHEMA_VERSION,
        "run_id": effective_run_id,
        "files_by_type": files_by_type,
        "findings_by_severity": findings_by_severity,
        "quarantined_row_count": len(quarantined_rows),
        "quarantined_file_count": len(quarantined_files),
        "point_count": len(points),
        "field_code_rule_count": len(field_code_rules),
        "dxf_entity_count": len(dxf_entities),
    }
    _write_json(qc_summary_json, qc_summary_payload)

    _write_json(
        quarantined_files_json,
        {
            "schema_version": SCHEMA_VERSION,
            "run_id": effective_run_id,
            "files": [asdict(item) for item in quarantined_files],
        },
    )

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

    manifest_payload = {
        "schema_version": SCHEMA_VERSION,
        "tool_version": __version__,
        "run_id": effective_run_id,
        "started_at": started_at,
        "ended_at": ended_at,
        "warning_threshold": warning_threshold,
        "warning_threshold_exceeded": warning_threshold_exceeded,
        "config": {
            "version": config["version"],
            "crd_mode": config["crd"]["mode"],
            "outputs": config["outputs"]["formats"],
        },
        "input_files": [asdict(record) for record in input_file_records],
        "summary": asdict(summary),
        "artifacts": {
            "normalized_points_csv": str(points_csv),
            "normalized_points_parquet": str(points_parquet),
            "normalized_field_code_rules_csv": str(field_rules_csv),
            "normalized_dxf_entities_csv": str(dxf_entities_csv),
            "qc_findings_jsonl": str(findings_jsonl),
            "qc_summary_json": str(qc_summary_json),
            "quarantined_rows_csv": str(quarantined_rows_csv),
            "quarantined_files_json": str(quarantined_files_json),
        },
    }
    _write_json(run_manifest_json, manifest_payload)

    has_warnings = bool(findings) or bool(quarantined_files) or bool(quarantined_rows)
    if fatal_issue:
        exit_code = 3
    elif has_warnings:
        exit_code = 2
    else:
        exit_code = 0

    artifacts = RunArtifacts(
        normalized_points_csv=str(points_csv),
        normalized_points_parquet=str(points_parquet),
        normalized_field_code_rules_csv=str(field_rules_csv),
        normalized_dxf_entities_csv=str(dxf_entities_csv),
        qc_findings_jsonl=str(findings_jsonl),
        qc_summary_json=str(qc_summary_json),
        quarantined_rows_csv=str(quarantined_rows_csv),
        quarantined_files_json=str(quarantined_files_json),
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
