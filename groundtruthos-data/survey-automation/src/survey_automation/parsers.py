from __future__ import annotations

import csv
import json
import re
from pathlib import Path

from .detection import is_field_header, is_point_header
from .models import DxfEntityRecord, FieldCodeRule, ParseResult, PointRecord, QCFinding, QuarantinedRow

_FIELD_CODE_COLUMNS = {
    "Field Code": "field_code",
    "Layer": "layer",
    "Symbol": "symbol",
    "Linework": "linework",
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


def _normalize_cell(cell: str, trim: bool) -> str:
    return cell.strip() if trim else cell


def _serialize_row(row: list[str]) -> str:
    return json.dumps(row, ensure_ascii=True)


def parse_csv_file(path: Path, file_type: str, config: dict, run_id: str) -> ParseResult:
    result = ParseResult()
    trim_strings = bool(config["normalization"]["trim_strings"])
    uppercase_field_code = bool(config["normalization"]["uppercase_field_code"])
    required_point_columns = list(config["validation"]["required_point_columns"])
    required_field_columns = list(config["validation"].get("required_field_code_columns", _FIELD_CODE_COLUMNS))
    point_column_index = {column: idx for idx, column in enumerate(required_point_columns)}
    field_column_index = {column: idx for idx, column in enumerate(required_field_columns)}
    expected_point_column_count = len(required_point_columns)
    expected_field_column_count = len(required_field_columns)

    section: str | None = None
    if file_type == "point_csv":
        section = "point"
    elif file_type == "field_code_csv":
        section = "field"

    with path.open("r", encoding="utf-8-sig", errors="replace", newline="") as handle:
        reader = csv.reader(handle)
        for row_number, raw_row in enumerate(reader, start=1):
            if not raw_row or not any(cell.strip() for cell in raw_row):
                continue

            row = [_normalize_cell(cell, trim_strings) for cell in raw_row]

            if is_point_header(row):
                section = "point"
                continue

            if is_field_header(row):
                section = "field"
                continue

            if section == "point":
                if len(row) != expected_point_column_count:
                    result.quarantined_rows.append(
                        QuarantinedRow(
                            file_path=str(path),
                            row_number=row_number,
                            reason="bad_column_count",
                            raw_row=_serialize_row(row),
                            details=f"expected {expected_point_column_count} columns for point row",
                        )
                    )
                    result.findings.append(
                        _mk_finding(
                            run_id=run_id,
                            severity="warning",
                            code="bad_column_count",
                            message="Point row has unexpected number of columns",
                            file_path=str(path),
                            row_number=row_number,
                        )
                    )
                    continue

                try:
                    point = PointRecord(
                        point_id=row[point_column_index["Point#"]],
                        northing=float(row[point_column_index["Northing"]]),
                        easting=float(row[point_column_index["Easting"]]),
                        elevation=float(row[point_column_index["Elevation"]]),
                        description=row[point_column_index["Description"]],
                        dwg_description=row[point_column_index["DWG Description"]],
                        dwg_layer=row[point_column_index["DWG Layer"]],
                        locked=row[point_column_index["Locked"]],
                        group_name=row[point_column_index["Group"]],
                        category=row[point_column_index["Category"]],
                        ls_number=row[point_column_index["LS Number"]],
                        source_file=str(path),
                        source_line=row_number,
                    )
                except ValueError:
                    result.quarantined_rows.append(
                        QuarantinedRow(
                            file_path=str(path),
                            row_number=row_number,
                            reason="invalid_numeric",
                            raw_row=_serialize_row(row),
                            details="northing/easting/elevation must be numeric",
                        )
                    )
                    result.findings.append(
                        _mk_finding(
                            run_id=run_id,
                            severity="warning",
                            code="invalid_numeric",
                            message="Point row has invalid numeric coordinate data",
                            file_path=str(path),
                            row_number=row_number,
                        )
                    )
                    continue

                if not point.point_id:
                    result.quarantined_rows.append(
                        QuarantinedRow(
                            file_path=str(path),
                            row_number=row_number,
                            reason="missing_point_id",
                            raw_row=_serialize_row(row),
                            details="point_id is required",
                        )
                    )
                    result.findings.append(
                        _mk_finding(
                            run_id=run_id,
                            severity="warning",
                            code="missing_point_id",
                            message="Point row missing point identifier",
                            file_path=str(path),
                            row_number=row_number,
                        )
                    )
                    continue

                result.points.append(point)
                continue

            if section == "field":
                if len(row) != expected_field_column_count:
                    result.quarantined_rows.append(
                        QuarantinedRow(
                            file_path=str(path),
                            row_number=row_number,
                            reason="bad_column_count",
                            raw_row=_serialize_row(row),
                            details=f"expected {expected_field_column_count} columns for field code rule",
                        )
                    )
                    result.findings.append(
                        _mk_finding(
                            run_id=run_id,
                            severity="warning",
                            code="bad_column_count",
                            message="Field code row has unexpected number of columns",
                            file_path=str(path),
                            row_number=row_number,
                        )
                    )
                    continue

                field_code = row[field_column_index["Field Code"]]
                if uppercase_field_code:
                    field_code = field_code.upper()
                if not field_code:
                    result.quarantined_rows.append(
                        QuarantinedRow(
                            file_path=str(path),
                            row_number=row_number,
                            reason="missing_field_code",
                            raw_row=_serialize_row(row),
                            details="field_code is required",
                        )
                    )
                    result.findings.append(
                        _mk_finding(
                            run_id=run_id,
                            severity="warning",
                            code="missing_field_code",
                            message="Field code rule row missing field code",
                            file_path=str(path),
                            row_number=row_number,
                        )
                    )
                    continue

                result.field_code_rules.append(
                    FieldCodeRule(
                        field_code=field_code,
                        layer=row[field_column_index["Layer"]],
                        symbol=row[field_column_index["Symbol"]],
                        linework=row[field_column_index["Linework"]],
                        source_file=str(path),
                        source_line=row_number,
                    )
                )
                continue

            result.quarantined_rows.append(
                QuarantinedRow(
                    file_path=str(path),
                    row_number=row_number,
                    reason="unknown_schema",
                    raw_row=_serialize_row(row),
                    details="row encountered before any recognized header",
                )
            )
            result.findings.append(
                _mk_finding(
                    run_id=run_id,
                    severity="warning",
                    code="unknown_schema",
                    message="Row does not match known schema and no section is active",
                    file_path=str(path),
                    row_number=row_number,
                )
            )

    return result


def _safe_float(value: str) -> float | None:
    try:
        return float(value)
    except ValueError:
        return None


def parse_ascii_dxf(path: Path, run_id: str) -> ParseResult:
    result = ParseResult()

    def iter_pairs() -> tuple[str, str]:
        with path.open("r", encoding="latin-1", errors="replace", newline="") as handle:
            while True:
                code_line = handle.readline()
                if not code_line:
                    break
                value_line = handle.readline()
                if not value_line:
                    break
                yield code_line.strip(), value_line.strip()

    in_entities = False
    pending_section_name = False
    current: dict[str, str | float | None] | None = None

    def flush_current() -> None:
        nonlocal current
        if current is None:
            return
        result.dxf_entities.append(
            DxfEntityRecord(
                entity_type=str(current.get("entity_type") or ""),
                layer=str(current.get("layer") or ""),
                x=current.get("x") if isinstance(current.get("x"), (float, type(None))) else None,
                y=current.get("y") if isinstance(current.get("y"), (float, type(None))) else None,
                z=current.get("z") if isinstance(current.get("z"), (float, type(None))) else None,
                text=str(current.get("text") or ""),
                handle=str(current.get("handle") or ""),
                source_file=str(path),
            )
        )
        current = None

    for code, value in iter_pairs():
        if pending_section_name and code == "2":
            in_entities = value.upper() == "ENTITIES"
            pending_section_name = False
            continue
        if pending_section_name:
            pending_section_name = False

        if code == "0" and value == "SECTION":
            pending_section_name = True
            continue

        if code == "0" and value == "ENDSEC":
            in_entities = False
            flush_current()
            continue

        if not in_entities:
            continue

        if code == "0":
            flush_current()
            current = {
                "entity_type": value,
                "layer": "",
                "x": None,
                "y": None,
                "z": None,
                "text": "",
                "handle": "",
            }
            continue

        if current is None:
            continue

        if code == "8":
            current["layer"] = value
        elif code == "10":
            current["x"] = _safe_float(value)
        elif code == "20":
            current["y"] = _safe_float(value)
        elif code == "30":
            current["z"] = _safe_float(value)
        elif code == "1":
            current["text"] = value
        elif code == "5":
            current["handle"] = value

    flush_current()

    if not result.dxf_entities:
        result.findings.append(
            _mk_finding(
                run_id=run_id,
                severity="warning",
                code="dxf_no_entities",
                message="ASCII DXF parsed but no entities were extracted",
                file_path=str(path),
                row_number=None,
            )
        )

    return result


_COORD_RE = re.compile(r"^[+-]?(?:\d+\.?\d*|\d*\.\d+)$")


def parse_text_crd(path: Path, run_id: str) -> ParseResult:
    result = ParseResult()

    with path.open("r", encoding="utf-8", errors="replace") as handle:
        for row_number, line in enumerate(handle, start=1):
            stripped = line.strip()
            if not stripped or stripped.startswith("#"):
                continue

            tokens = [token for token in re.split(r"[\s,]+", stripped) if token]
            if len(tokens) < 4:
                result.quarantined_rows.append(
                    QuarantinedRow(
                        file_path=str(path),
                        row_number=row_number,
                        reason="bad_column_count",
                        raw_row=stripped,
                        details="expected at least point_id northing easting elevation",
                    )
                )
                result.findings.append(
                    _mk_finding(
                        run_id=run_id,
                        severity="warning",
                        code="bad_column_count",
                        message="CRD text row has too few tokens",
                        file_path=str(path),
                        row_number=row_number,
                    )
                )
                continue

            point_id = tokens[0]
            northing_raw, easting_raw, elevation_raw = tokens[1], tokens[2], tokens[3]
            if not (_COORD_RE.match(northing_raw) and _COORD_RE.match(easting_raw) and _COORD_RE.match(elevation_raw)):
                result.quarantined_rows.append(
                    QuarantinedRow(
                        file_path=str(path),
                        row_number=row_number,
                        reason="invalid_numeric",
                        raw_row=stripped,
                        details="northing/easting/elevation must be numeric",
                    )
                )
                result.findings.append(
                    _mk_finding(
                        run_id=run_id,
                        severity="warning",
                        code="invalid_numeric",
                        message="CRD text row has invalid coordinate numbers",
                        file_path=str(path),
                        row_number=row_number,
                    )
                )
                continue

            description = " ".join(tokens[4:]) if len(tokens) > 4 else ""
            result.points.append(
                PointRecord(
                    point_id=point_id,
                    northing=float(northing_raw),
                    easting=float(easting_raw),
                    elevation=float(elevation_raw),
                    description=description,
                    dwg_description="",
                    dwg_layer="",
                    locked="",
                    group_name="",
                    category="",
                    ls_number="",
                    source_file=str(path),
                    source_line=row_number,
                )
            )

    return result
