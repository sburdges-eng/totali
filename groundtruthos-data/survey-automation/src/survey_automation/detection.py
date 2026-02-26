from __future__ import annotations

import csv
import re
from pathlib import Path

POINT_HEADER_PREFIX = ["point#", "northing", "easting", "elevation"]
FIELD_HEADER = ["field code", "layer", "symbol", "linework"]
_TEXT_ROW_SPLIT_RE = re.compile(r"[\s,]+")


def _normalize_row(row: list[str]) -> list[str]:
    return [cell.strip().lower() for cell in row]


def is_point_header(row: list[str]) -> bool:
    normalized = _normalize_row(row)
    return len(normalized) >= 4 and normalized[:4] == POINT_HEADER_PREFIX


def is_field_header(row: list[str]) -> bool:
    normalized = _normalize_row(row)
    return len(normalized) >= 4 and normalized[:4] == FIELD_HEADER


def classify_csv(path: Path) -> tuple[str, str]:
    point_headers = 0
    field_headers = 0
    non_empty_rows = 0

    with path.open("r", encoding="utf-8-sig", errors="replace", newline="") as handle:
        reader = csv.reader(handle)
        for row in reader:
            if not row or not any(cell.strip() for cell in row):
                continue
            non_empty_rows += 1
            if is_point_header(row):
                point_headers += 1
            elif is_field_header(row):
                field_headers += 1

    if non_empty_rows == 0:
        return "mixed_csv", "csv_empty_or_unknown"

    if point_headers > 0 and field_headers > 0:
        return "mixed_csv", "csv_contains_multiple_schema_headers"

    if point_headers > 0:
        if point_headers > 1:
            return "mixed_csv", "csv_repeated_point_headers"
        return "point_csv", "point_header_detected"

    if field_headers > 0:
        if field_headers > 1:
            return "mixed_csv", "csv_repeated_field_headers"
        return "field_code_csv", "field_code_header_detected"

    return "mixed_csv", "csv_unknown_header"


def _looks_binary(data: bytes) -> bool:
    if not data:
        return False
    if b"\x00" in data:
        return True
    text_bytes = sum(1 for b in data if 32 <= b <= 126 or b in {9, 10, 13})
    ratio = text_bytes / max(len(data), 1)
    return ratio < 0.9


def classify_dxf(path: Path) -> tuple[str, str]:
    sample = path.read_bytes()[:512]
    if sample.startswith(b"AutoCAD Binary DXF"):
        return "binary_dxf", "binary_dxf_signature"
    if _looks_binary(sample):
        return "binary_dxf", "binary_dxf_binary_payload"
    return "ascii_dxf", "ascii_dxf_detected"


def classify_crd(path: Path) -> tuple[str, str]:
    sample = path.read_bytes()[:1024]
    if b"New CRD Format2" in sample:
        return "crd_binary", "carlson_binary_crd_signature"
    if _looks_binary(sample):
        return "crd_binary", "binary_like_crd_payload"
    return "crd_text", "text_crd_detected"


def _is_float_token(token: str) -> bool:
    try:
        float(token)
        return True
    except ValueError:
        return False


def classify_text_points(path: Path) -> tuple[str, str]:
    data = path.read_bytes()
    if _looks_binary(data[:1024]):
        return "unsupported", "text_points_binary_payload"

    non_empty_lines = 0
    point_like_rows = 0

    text = data.decode("utf-8", errors="replace")
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue

        non_empty_lines += 1
        tokens = [token for token in _TEXT_ROW_SPLIT_RE.split(stripped) if token]
        if len(tokens) >= 4 and _is_float_token(tokens[1]) and _is_float_token(tokens[2]) and _is_float_token(tokens[3]):
            point_like_rows += 1

        if non_empty_lines >= 20:
            break

    if point_like_rows > 0:
        return "crd_text", "text_points_detected"
    if non_empty_lines == 0:
        return "unsupported", "text_points_empty_or_unknown"
    return "unsupported", "text_points_unknown_schema"


def detect_file_type(path: Path) -> tuple[str, str]:
    ext = path.suffix.lower()

    if ext == ".csv":
        return classify_csv(path)
    if ext == ".dxf":
        return classify_dxf(path)
    if ext == ".crd":
        return classify_crd(path)
    if ext in {".txt", ".pts", ".asc"}:
        return classify_text_points(path)

    if ext in {".dwg", ".pcs", ".bak", ".ini"}:
        return "unsupported", f"unsupported_extension:{ext}"

    return "unsupported", f"unknown_extension:{ext or 'none'}"


def discover_files(
    input_dir: Path,
    include_globs: list[str],
    exclude_globs: list[str],
) -> list[Path]:
    included_by_real_path: dict[Path, Path] = {}
    for pattern in include_globs:
        for candidate in input_dir.glob(pattern):
            if candidate.is_file():
                resolved = candidate.resolve()
                existing = included_by_real_path.get(resolved)
                if existing is None or candidate.as_posix() < existing.as_posix():
                    included_by_real_path[resolved] = candidate

    excluded_real_paths: set[Path] = set()
    for pattern in exclude_globs:
        for candidate in input_dir.glob(pattern):
            if candidate.is_file():
                excluded_real_paths.add(candidate.resolve())

    filtered = [
        path
        for resolved, path in included_by_real_path.items()
        if resolved not in excluded_real_paths
    ]
    filtered.sort(key=lambda p: p.as_posix())
    return filtered
