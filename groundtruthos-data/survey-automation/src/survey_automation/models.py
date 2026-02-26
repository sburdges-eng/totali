from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


SCHEMA_VERSION = "1.0.0"


@dataclass(slots=True)
class InputFileRecord:
    file_path: str
    detected_type: str
    size_bytes: int
    status: str
    message: str = ""


@dataclass(slots=True)
class PointRecord:
    point_id: str
    northing: float
    easting: float
    elevation: float
    description: str
    dwg_description: str
    dwg_layer: str
    locked: str
    group_name: str
    category: str
    ls_number: str
    source_file: str
    source_line: int


@dataclass(slots=True)
class FieldCodeRule:
    field_code: str
    layer: str
    symbol: str
    linework: str
    source_file: str
    source_line: int


@dataclass(slots=True)
class DxfEntityRecord:
    entity_type: str
    layer: str
    x: float | None
    y: float | None
    z: float | None
    text: str
    handle: str
    source_file: str


@dataclass(slots=True)
class QuarantinedRow:
    file_path: str
    row_number: int
    reason: str
    raw_row: str
    details: str = ""


@dataclass(slots=True)
class QuarantinedFile:
    file_path: str
    reason: str
    message: str


@dataclass(slots=True)
class QCFinding:
    # Assigned by pipeline._finalize_findings after findings are collected and sorted.
    finding_id: str
    severity: str
    code: str
    message: str
    file_path: str
    row_number: int | None
    run_id: str


@dataclass(slots=True)
class RunSummary:
    run_id: str
    started_at: str
    ended_at: str
    files_total: int
    files_processed: int
    files_quarantined: int
    findings_by_severity: dict[str, int] = field(default_factory=dict)


@dataclass(slots=True)
class ParseResult:
    points: list[PointRecord] = field(default_factory=list)
    field_code_rules: list[FieldCodeRule] = field(default_factory=list)
    dxf_entities: list[DxfEntityRecord] = field(default_factory=list)
    quarantined_rows: list[QuarantinedRow] = field(default_factory=list)
    findings: list[QCFinding] = field(default_factory=list)


@dataclass(slots=True)
class RunArtifacts:
    normalized_points_csv: str
    normalized_points_parquet: str
    normalized_field_code_rules_csv: str
    normalized_dxf_entities_csv: str
    qc_findings_jsonl: str
    qc_summary_json: str
    quarantined_rows_csv: str
    quarantined_files_json: str
    run_manifest_json: str


def as_json_dict(data: Any) -> dict[str, Any]:
    """Small helper for JSON output with schema version."""
    if isinstance(data, dict):
        payload = dict(data)
    else:
        payload = {"value": data}
    payload.setdefault("schema_version", SCHEMA_VERSION)
    return payload
