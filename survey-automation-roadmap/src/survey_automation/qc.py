from __future__ import annotations

import re
from collections import Counter

from .models import FieldCodeRule, PointRecord, QCFinding

_POINT_COLUMN_TO_ATTR = {
    "Point#": "point_id",
    "Northing": "northing",
    "Easting": "easting",
    "Elevation": "elevation",
    "Description": "description",
    "DWG Description": "dwg_description",
    "DWG Layer": "dwg_layer",
    "Locked": "locked",
    "Group": "group_name",
    "Category": "category",
    "LS Number": "ls_number",
}


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


def _parse_bound(bounds_cfg: dict | None, key: str) -> tuple[float | None, float | None]:
    if not isinstance(bounds_cfg, dict):
        return None, None

    if key in bounds_cfg and isinstance(bounds_cfg[key], dict):
        min_value = bounds_cfg[key].get("min")
        max_value = bounds_cfg[key].get("max")
    else:
        min_value = bounds_cfg.get(f"{key}_min")
        max_value = bounds_cfg.get(f"{key}_max")

    try:
        min_bound = float(min_value) if min_value is not None else None
    except (TypeError, ValueError):
        min_bound = None

    try:
        max_bound = float(max_value) if max_value is not None else None
    except (TypeError, ValueError):
        max_bound = None

    return min_bound, max_bound


_CODE_RE = re.compile(r"^[A-Z0-9]+$")


def extract_description_code(description: str) -> str:
    normalized = description.strip().upper()
    if not normalized:
        return ""
    first_token = normalized.split()[0].strip(".,;:()[]{}")
    return first_token


def run_qc(
    points: list[PointRecord],
    field_code_rules: list[FieldCodeRule],
    config: dict,
    run_id: str,
) -> list[QCFinding]:
    findings: list[QCFinding] = []

    required_columns: list[str] = config["validation"]["required_point_columns"]
    unknown_required_columns = sorted(column for column in required_columns if column not in _POINT_COLUMN_TO_ATTR)
    for column in unknown_required_columns:
        findings.append(
            _mk_finding(
                run_id=run_id,
                severity="warning",
                code="unknown_required_point_column",
                message=f"Configured required point column is not recognized: `{column}`",
                file_path="",
                row_number=None,
            )
        )

    bounds_cfg = config["validation"].get("coordinate_bounds")
    for point in points:
        for key, value in [("northing", point.northing), ("easting", point.easting), ("elevation", point.elevation)]:
            min_bound, max_bound = _parse_bound(bounds_cfg, key)
            if min_bound is not None and value < min_bound:
                findings.append(
                    _mk_finding(
                        run_id=run_id,
                        severity="warning",
                        code="coordinate_out_of_bounds",
                        message=f"{key} is below configured minimum",
                        file_path=point.source_file,
                        row_number=point.source_line,
                    )
                )
            if max_bound is not None and value > max_bound:
                findings.append(
                    _mk_finding(
                        run_id=run_id,
                        severity="warning",
                        code="coordinate_out_of_bounds",
                        message=f"{key} is above configured maximum",
                        file_path=point.source_file,
                        row_number=point.source_line,
                    )
                )

    duplicate_mode = config["validation"].get("duplicate_point_id_mode", "all_occurrences")

    if duplicate_mode == "within_file":
        file_point_ids: dict[tuple[str, str], list[PointRecord]] = {}
        for point in points:
            key = (point.source_file, point.point_id)
            file_point_ids.setdefault(key, []).append(point)
        for (source_file, point_id), occurrences in file_point_ids.items():
            if len(occurrences) < 2:
                continue
            first_occurrence = min(occurrences, key=lambda item: item.source_line)
            findings.append(
                _mk_finding(
                    run_id=run_id,
                    severity="warning",
                    code="duplicate_point_id",
                    message=f"Duplicate point id `{point_id}` detected ({len(occurrences)} occurrences in file)",
                    file_path=source_file,
                    row_number=first_occurrence.source_line,
                )
            )
    else:
        point_ids: dict[str, list[PointRecord]] = {}
        for point in points:
            point_ids.setdefault(point.point_id, []).append(point)
        for point_id, occurrences in point_ids.items():
            if len(occurrences) < 2:
                continue
            if duplicate_mode == "per_point_id":
                first_occurrence = min(occurrences, key=lambda item: (item.source_file, item.source_line))
                findings.append(
                    _mk_finding(
                        run_id=run_id,
                        severity="warning",
                        code="duplicate_point_id",
                        message=f"Duplicate point id `{point_id}` detected ({len(occurrences)} occurrences)",
                        file_path=first_occurrence.source_file,
                        row_number=first_occurrence.source_line,
                    )
                )
                continue

            for occurrence in occurrences:
                findings.append(
                    _mk_finding(
                        run_id=run_id,
                        severity="warning",
                        code="duplicate_point_id",
                        message=f"Duplicate point id `{point_id}` detected ({len(occurrences)} occurrences)",
                        file_path=occurrence.source_file,
                        row_number=occurrence.source_line,
                    )
                )

    rule_codes = {rule.field_code.strip().upper() for rule in field_code_rules if rule.field_code.strip()}
    if not rule_codes:
        findings.append(
            _mk_finding(
                run_id=run_id,
                severity="warning",
                code="no_field_code_rules",
                message="No field code rules were parsed; mapping coverage cannot be fully validated",
                file_path="",
                row_number=None,
            )
        )
        return findings

    skip_unmapped_categories = {
        category.strip().upper()
        for category in config["validation"].get("unmapped_description_skip_categories", [])
        if category.strip()
    }
    missing_codes: dict[str, PointRecord] = {}
    for point in points:
        if point.category.strip().upper() in skip_unmapped_categories:
            continue
        code = extract_description_code(point.description)
        if not code:
            continue
        if code in rule_codes:
            continue
        if not _CODE_RE.match(code):
            continue
        missing_codes.setdefault(code, point)

    for missing_code in sorted(missing_codes):
        point = missing_codes[missing_code]
        findings.append(
            _mk_finding(
                run_id=run_id,
                severity="warning",
                code="description_code_unmapped",
                message=f"Description code `{missing_code}` has no field-code mapping",
                file_path=point.source_file,
                row_number=point.source_line,
            )
        )

    return findings


def summarize_findings(findings: list[QCFinding]) -> dict[str, int]:
    counts = Counter(f.severity for f in findings)
    return dict(sorted(counts.items()))
