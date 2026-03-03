from __future__ import annotations

import csv
import hashlib
import json
import math
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from .json_contract import (
    build_contract_payload,
    build_invariant,
    is_absolute_path_value,
    to_stable_relative_path,
    write_contract_json,
)

_RULE_TYPES = ("exact_code", "exact_phrase", "regex", "prefix")
_EXACT_ROUTE_TYPES = {"exact_code", "exact_phrase"}
_PATTERN_ROUTE_TYPES = {"regex", "prefix"}
_FEATURE_TYPES = {"point", "line_string", "polygon"}
_GROUP_BY_VALUES = {"code", "code_and_source", "per_point"}
_QUARANTINE_REASONS = {"unmatched", "ambiguous_match", "topology_invalid", "contract_violation"}
_EPSILON = 1e-9


@dataclass(slots=True)
class BridgeRow:
    row_ref: str
    row_index: int
    point_id: str
    northing: float
    easting: float
    elevation: float
    description: str
    description_norm: str
    source_file: str
    source_alias: str
    source_line: int


@dataclass(slots=True)
class BridgeRule:
    rule_id: str
    rule_type: str
    feature_code: str
    feature_type: str
    group_by: str
    match_value: str | None = None
    pattern: str | None = None
    compiled_pattern: re.Pattern[str] | None = None


@dataclass(slots=True)
class BridgeRunResult:
    run_id: str
    run_root: Path
    intent_path: Path
    geometry_path: Path
    bridge_manifest_path: Path
    mapped_points: int
    unmapped_points: int
    quarantined_points: int
    intent_features: int
    geometry_features: int


class BridgeConfigError(ValueError):
    pass


def _normalize_text(value: str) -> str:
    return " ".join(value.strip().upper().split())


def _parse_int(value: str, *, field_name: str) -> int:
    try:
        return int(value)
    except ValueError:
        try:
            return int(float(value))
        except ValueError as exc:
            raise BridgeConfigError(f"Invalid integer for `{field_name}`: {value}") from exc


def _parse_float(value: str, *, field_name: str) -> float:
    try:
        return float(value)
    except ValueError as exc:
        raise BridgeConfigError(f"Invalid float for `{field_name}`: {value}") from exc


def _sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _sha256_file(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def _sha256_json_data(payload: dict[str, Any]) -> str:
    encoded = json.dumps(payload, ensure_ascii=True, separators=(",", ":")).encode("utf-8")
    return _sha256_bytes(encoded)


def _run_relative(run_root: Path, path: Path) -> str:
    relative = to_stable_relative_path(path, base=run_root)
    if is_absolute_path_value(relative):
        raise BridgeConfigError(f"Expected relative path, got absolute path: {relative}")
    return relative


def _read_json_dict(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise BridgeConfigError(f"Failed to read JSON: {path} ({exc})") from exc
    if not isinstance(payload, dict):
        raise BridgeConfigError(f"Expected JSON object at: {path}")
    return payload


def _read_snapshot_id(run_root: Path) -> str:
    snapshot_path = run_root / "manifest" / "dataset_snapshot.json"
    if not snapshot_path.exists():
        raise BridgeConfigError(
            f"Missing dataset snapshot artifact: {snapshot_path}. Run pipeline before bridge execution."
        )
    payload = _read_json_dict(snapshot_path)
    data = payload.get("data")
    if isinstance(data, dict):
        snapshot_id = data.get("snapshot_id")
        if isinstance(snapshot_id, str) and snapshot_id.strip():
            return snapshot_id.strip()
    legacy_snapshot_id = payload.get("snapshot_id")
    if isinstance(legacy_snapshot_id, str) and legacy_snapshot_id.strip():
        return legacy_snapshot_id.strip()
    raise BridgeConfigError("dataset_snapshot.json is missing `data.snapshot_id`")


def _read_input_rel_paths(run_root: Path) -> set[str]:
    run_manifest_path = run_root / "manifest" / "run_manifest.json"
    if not run_manifest_path.exists():
        return set()

    payload = _read_json_dict(run_manifest_path)
    data = payload.get("data")
    input_files: list[dict[str, Any]] = []
    if isinstance(data, dict) and isinstance(data.get("input_files"), list):
        input_files = [item for item in data["input_files"] if isinstance(item, dict)]
    elif isinstance(payload.get("input_files"), list):
        input_files = [item for item in payload["input_files"] if isinstance(item, dict)]

    rel_paths: set[str] = set()
    for item in input_files:
        rel_path = item.get("file_path")
        if isinstance(rel_path, str) and rel_path.strip():
            rel_paths.add(Path(rel_path.strip()).as_posix())
    return rel_paths


def _source_alias(source_file: str, known_rel_paths: set[str]) -> str:
    normalized = source_file.replace("\\", "/").strip()
    if not normalized:
        return ""

    normalized_posix = Path(normalized).as_posix()
    if not is_absolute_path_value(normalized_posix):
        return normalized_posix

    candidates = [
        rel
        for rel in known_rel_paths
        if normalized_posix == rel or normalized_posix.endswith("/" + rel)
    ]
    if candidates:
        return max(candidates, key=len)
    return Path(normalized_posix).name


def _load_points(points_path: Path, known_rel_paths: set[str]) -> list[BridgeRow]:
    if not points_path.exists():
        raise BridgeConfigError(f"Missing normalized points artifact: {points_path}")

    rows: list[BridgeRow] = []
    with points_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        required_columns = {
            "point_id",
            "northing",
            "easting",
            "elevation",
            "description",
            "source_file",
            "source_line",
        }
        missing = sorted(required_columns - set(reader.fieldnames or []))
        if missing:
            raise BridgeConfigError("points.csv is missing required columns: " + ", ".join(missing))

        for idx, row in enumerate(reader, start=1):
            point_id = (row.get("point_id") or "").strip()
            if not point_id:
                raise BridgeConfigError(f"Row {idx} is missing `point_id`")

            description = (row.get("description") or "").strip()
            source_file = (row.get("source_file") or "").strip()
            source_alias = _source_alias(source_file, known_rel_paths)
            source_line = _parse_int((row.get("source_line") or "0").strip(), field_name="source_line")

            bridge_row = BridgeRow(
                row_ref="",
                row_index=idx,
                point_id=point_id,
                northing=_parse_float((row.get("northing") or "").strip(), field_name="northing"),
                easting=_parse_float((row.get("easting") or "").strip(), field_name="easting"),
                elevation=_parse_float((row.get("elevation") or "").strip(), field_name="elevation"),
                description=description,
                description_norm=_normalize_text(description),
                source_file=source_file,
                source_alias=source_alias,
                source_line=source_line,
            )
            rows.append(bridge_row)

    rows.sort(
        key=lambda item: (
            item.source_alias,
            item.source_line,
            item.point_id,
            item.description_norm,
            item.row_index,
        )
    )
    for idx, row in enumerate(rows, start=1):
        row.row_ref = f"row-{idx:06d}"
    return rows


def _build_points_digest(rows: list[BridgeRow]) -> str:
    hasher = hashlib.sha256()
    for row in rows:
        hasher.update(
            (
                f"{row.row_ref}|{row.point_id}|{row.easting:.6f}|{row.northing:.6f}|"
                f"{row.elevation:.6f}|{row.description_norm}|{row.source_alias}|{row.source_line}"
            ).encode("utf-8")
        )
        hasher.update(b"\n")
    return hasher.hexdigest()


def _load_bridge_rules(rules_path: Path) -> tuple[str, str, list[str], dict[str, list[BridgeRule]]]:
    if not rules_path.exists():
        raise BridgeConfigError(f"Bridge rule pack does not exist: {rules_path}")

    raw = yaml.safe_load(rules_path.read_text(encoding="utf-8")) or {}
    if not isinstance(raw, dict):
        raise BridgeConfigError("Bridge rule pack root must be a YAML mapping")

    schema_version = raw.get("schemaVersion")
    if not isinstance(schema_version, str) or not schema_version.strip():
        raise BridgeConfigError("Bridge rule pack requires non-empty `schemaVersion`")

    metadata = raw.get("metadata")
    if not isinstance(metadata, dict):
        raise BridgeConfigError("Bridge rule pack requires `metadata` mapping")

    rule_pack_id = metadata.get("rulePackId")
    rule_pack_version = metadata.get("version")
    if not isinstance(rule_pack_id, str) or not rule_pack_id.strip():
        raise BridgeConfigError("Bridge rule pack requires non-empty `metadata.rulePackId`")
    if not isinstance(rule_pack_version, str) or not rule_pack_version.strip():
        raise BridgeConfigError("Bridge rule pack requires non-empty `metadata.version`")

    precedence_raw = raw.get("precedence", list(_RULE_TYPES))
    if not isinstance(precedence_raw, list) or not all(isinstance(item, str) for item in precedence_raw):
        raise BridgeConfigError("Bridge rule pack `precedence` must be list[str]")
    precedence = [item.strip() for item in precedence_raw if item.strip()]
    if not precedence:
        raise BridgeConfigError("Bridge rule pack `precedence` must include at least one rule type")

    unknown_rule_types = sorted(set(precedence) - set(_RULE_TYPES))
    if unknown_rule_types:
        raise BridgeConfigError(
            "Unsupported rule type(s) in precedence: " + ", ".join(unknown_rule_types)
        )
    if len(precedence) != len(set(precedence)):
        raise BridgeConfigError("Bridge rule pack `precedence` contains duplicate values")

    rules_root = raw.get("rules")
    if not isinstance(rules_root, dict):
        raise BridgeConfigError("Bridge rule pack requires `rules` mapping")

    parsed_rules: dict[str, list[BridgeRule]] = {rule_type: [] for rule_type in _RULE_TYPES}
    for rule_type in _RULE_TYPES:
        entries = rules_root.get(rule_type, [])
        if entries is None:
            entries = []
        if not isinstance(entries, list):
            raise BridgeConfigError(f"Bridge rule pack `rules.{rule_type}` must be a list")

        for idx, entry in enumerate(entries, start=1):
            if not isinstance(entry, dict):
                raise BridgeConfigError(f"Rule entry at `rules.{rule_type}[{idx}]` must be a mapping")

            rule_id = entry.get("id")
            if not isinstance(rule_id, str) or not rule_id.strip():
                rule_id = f"{rule_type}_{idx}"

            feature_code = entry.get("feature_code")
            if not isinstance(feature_code, str) or not feature_code.strip():
                raise BridgeConfigError(
                    f"Rule `{rule_id}` requires non-empty `feature_code`"
                )

            feature_type_raw = entry.get("feature_type", "point")
            if not isinstance(feature_type_raw, str) or not feature_type_raw.strip():
                raise BridgeConfigError(f"Rule `{rule_id}` has invalid `feature_type`")
            feature_type = feature_type_raw.strip().lower()
            if feature_type not in _FEATURE_TYPES:
                raise BridgeConfigError(
                    f"Rule `{rule_id}` has unsupported `feature_type`: {feature_type}"
                )

            group_by_raw = entry.get("group_by", "code")
            if not isinstance(group_by_raw, str) or not group_by_raw.strip():
                raise BridgeConfigError(f"Rule `{rule_id}` has invalid `group_by`")
            group_by = group_by_raw.strip().lower()
            if group_by not in _GROUP_BY_VALUES:
                raise BridgeConfigError(f"Rule `{rule_id}` has unsupported `group_by`: {group_by}")

            match_value: str | None = None
            pattern: str | None = None
            compiled_pattern: re.Pattern[str] | None = None
            if rule_type == "regex":
                pattern_raw = entry.get("pattern")
                if not isinstance(pattern_raw, str) or not pattern_raw.strip():
                    raise BridgeConfigError(f"Regex rule `{rule_id}` requires non-empty `pattern`")
                pattern = pattern_raw.strip()
                try:
                    compiled_pattern = re.compile(pattern)
                except re.error as exc:
                    raise BridgeConfigError(
                        f"Regex rule `{rule_id}` has invalid `pattern`: {exc}"
                    ) from exc
            else:
                match_raw = entry.get("match")
                if not isinstance(match_raw, str) or not match_raw.strip():
                    raise BridgeConfigError(f"Rule `{rule_id}` requires non-empty `match`")
                match_value = _normalize_text(match_raw)

            parsed_rules[rule_type].append(
                BridgeRule(
                    rule_id=rule_id.strip(),
                    rule_type=rule_type,
                    feature_code=feature_code.strip(),
                    feature_type=feature_type,
                    group_by=group_by,
                    match_value=match_value,
                    pattern=pattern,
                    compiled_pattern=compiled_pattern,
                )
            )

    return rule_pack_id.strip(), rule_pack_version.strip(), precedence, parsed_rules


def _rule_matches(rule: BridgeRule, row: BridgeRow, code_token: str) -> bool:
    if rule.rule_type == "exact_code":
        return bool(rule.match_value) and code_token == rule.match_value
    if rule.rule_type == "exact_phrase":
        return bool(rule.match_value) and row.description_norm == rule.match_value
    if rule.rule_type == "prefix":
        return bool(rule.match_value) and row.description_norm.startswith(rule.match_value)
    if rule.rule_type == "regex":
        return bool(rule.compiled_pattern and rule.compiled_pattern.search(row.description_norm))
    return False


def _resolve_group_key(rule: BridgeRule, row: BridgeRow) -> str:
    if rule.group_by == "code":
        return rule.feature_code
    if rule.group_by == "code_and_source":
        return f"{rule.feature_code}|{row.source_alias}"
    return f"{rule.feature_code}|{row.source_alias}|{row.source_line}|{row.point_id}|{row.row_ref}"


def _sort_route_record(record: dict[str, Any]) -> tuple[Any, ...]:
    return (
        record.get("row_ref", ""),
        record.get("point_id", ""),
        record.get("reason", ""),
        record.get("rule_id", ""),
        record.get("feature_id", ""),
    )


def _classify_rows(
    rows: list[BridgeRow],
    precedence: list[str],
    rules_by_type: dict[str, list[BridgeRule]],
) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]], dict[str, BridgeRow]]:
    groups: dict[tuple[str, str, str], dict[str, Any]] = {}
    row_lookup: dict[str, BridgeRow] = {row.row_ref: row for row in rows}

    accepted_exact_records: list[dict[str, Any]] = []
    accepted_pattern_records: list[dict[str, Any]] = []
    quarantined_records: list[dict[str, Any]] = []

    for row in rows:
        code_token = row.description_norm.split(" ", 1)[0] if row.description_norm else ""
        resolved = False

        for rule_type in precedence:
            candidates = [
                rule
                for rule in rules_by_type.get(rule_type, [])
                if _rule_matches(rule, row, code_token)
            ]
            if not candidates:
                continue

            if len(candidates) > 1:
                quarantined_records.append(
                    {
                        "row_ref": row.row_ref,
                        "point_id": row.point_id,
                        "reason": "ambiguous_match",
                        "detail": f"Multiple {rule_type} rules matched",
                        "candidate_rule_ids": sorted(rule.rule_id for rule in candidates),
                    }
                )
                resolved = True
                break

            winner = candidates[0]
            group_key = _resolve_group_key(winner, row)
            bucket_key = (winner.feature_code, winner.feature_type, group_key)
            bucket = groups.get(bucket_key)
            if bucket is None:
                bucket = {
                    "feature_code": winner.feature_code,
                    "feature_type": winner.feature_type,
                    "group_key": group_key,
                    "point_refs": [],
                    "matched_rule_ids": set(),
                }
                groups[bucket_key] = bucket

            bucket["point_refs"].append(row.row_ref)
            bucket["matched_rule_ids"].add(winner.rule_id)

            accepted_record = {
                "row_ref": row.row_ref,
                "point_id": row.point_id,
                "rule_type": winner.rule_type,
                "rule_id": winner.rule_id,
                "feature_code": winner.feature_code,
                "feature_type": winner.feature_type,
                "group_key": group_key,
                "feature_id": "",
            }
            if winner.rule_type in _EXACT_ROUTE_TYPES:
                accepted_exact_records.append(accepted_record)
            else:
                accepted_pattern_records.append(accepted_record)
            resolved = True
            break

        if not resolved:
            quarantined_records.append(
                {
                    "row_ref": row.row_ref,
                    "point_id": row.point_id,
                    "reason": "unmatched",
                    "detail": "No mapping rule matched description",
                    "candidate_rule_ids": [],
                }
            )

    intent_features: list[dict[str, Any]] = []
    bucket_to_feature_id: dict[tuple[str, str, str], str] = {}
    for idx, (bucket_key, bucket) in enumerate(sorted(groups.items(), key=lambda item: item[0]), start=1):
        feature_id = f"intent-{idx:06d}"
        bucket_to_feature_id[bucket_key] = feature_id
        intent_features.append(
            {
                "feature_id": feature_id,
                "feature_code": bucket["feature_code"],
                "feature_type": bucket["feature_type"],
                "group_key": bucket["group_key"],
                "point_refs": list(bucket["point_refs"]),
                "matched_rule_ids": sorted(bucket["matched_rule_ids"]),
            }
        )

    for record in accepted_exact_records + accepted_pattern_records:
        bucket_key = (record["feature_code"], record["feature_type"], record["group_key"])
        record["feature_id"] = bucket_to_feature_id.get(bucket_key, "")

    accepted_exact_records.sort(key=_sort_route_record)
    accepted_pattern_records.sort(key=_sort_route_record)
    quarantined_records.sort(key=_sort_route_record)

    routing = {
        "accepted_exact": {
            "count": len(accepted_exact_records),
            "records": accepted_exact_records,
        },
        "accepted_pattern": {
            "count": len(accepted_pattern_records),
            "records": accepted_pattern_records,
        },
        "quarantined": {
            "count": len(quarantined_records),
            "records": quarantined_records,
        },
    }

    return intent_features, routing, row_lookup


def _orientation(a: tuple[float, float], b: tuple[float, float], c: tuple[float, float]) -> int:
    value = (b[0] - a[0]) * (c[1] - a[1]) - (b[1] - a[1]) * (c[0] - a[0])
    if abs(value) <= _EPSILON:
        return 0
    return 1 if value > 0 else -1


def _on_segment(a: tuple[float, float], b: tuple[float, float], c: tuple[float, float]) -> bool:
    return (
        min(a[0], b[0]) - _EPSILON <= c[0] <= max(a[0], b[0]) + _EPSILON
        and min(a[1], b[1]) - _EPSILON <= c[1] <= max(a[1], b[1]) + _EPSILON
    )


def _segments_intersect(
    p1: tuple[float, float],
    p2: tuple[float, float],
    q1: tuple[float, float],
    q2: tuple[float, float],
) -> bool:
    o1 = _orientation(p1, p2, q1)
    o2 = _orientation(p1, p2, q2)
    o3 = _orientation(q1, q2, p1)
    o4 = _orientation(q1, q2, p2)

    if o1 != o2 and o3 != o4:
        return True

    if o1 == 0 and _on_segment(p1, p2, q1):
        return True
    if o2 == 0 and _on_segment(p1, p2, q2):
        return True
    if o3 == 0 and _on_segment(q1, q2, p1):
        return True
    if o4 == 0 and _on_segment(q1, q2, p2):
        return True
    return False


def _line_has_self_intersection(points: list[tuple[float, float]]) -> bool:
    if len(points) < 4:
        return False

    segment_count = len(points) - 1
    for i in range(segment_count):
        p1 = points[i]
        p2 = points[i + 1]
        for j in range(i + 1, segment_count):
            if abs(i - j) <= 1:
                continue
            q1 = points[j]
            q2 = points[j + 1]
            if _segments_intersect(p1, p2, q1, q2):
                return True
    return False


def _ring_has_self_intersection(ring: list[tuple[float, float]]) -> bool:
    if len(ring) < 5:
        return False

    segment_count = len(ring) - 1
    for i in range(segment_count):
        p1 = ring[i]
        p2 = ring[i + 1]
        for j in range(i + 1, segment_count):
            if abs(i - j) <= 1:
                continue
            if i == 0 and j == segment_count - 1:
                continue
            q1 = ring[j]
            q2 = ring[j + 1]
            if _segments_intersect(p1, p2, q1, q2):
                return True
    return False


def _line_length_2d(points: list[tuple[float, float]]) -> float:
    total = 0.0
    for idx in range(len(points) - 1):
        x1, y1 = points[idx]
        x2, y2 = points[idx + 1]
        total += math.hypot(x2 - x1, y2 - y1)
    return total


def _polygon_area_2d(ring: list[tuple[float, float]]) -> float:
    accum = 0.0
    for idx in range(len(ring) - 1):
        x1, y1 = ring[idx]
        x2, y2 = ring[idx + 1]
        accum += (x1 * y2) - (x2 * y1)
    return abs(accum) * 0.5


def _build_geometry_features(
    intent_features: list[dict[str, Any]],
    row_lookup: dict[str, BridgeRow],
) -> tuple[list[dict[str, Any]], dict[str, str]]:
    geometry_features: list[dict[str, Any]] = []
    topology_rejections: dict[str, str] = {}

    for feature in intent_features:
        point_refs = list(feature["point_refs"])
        missing_refs = [ref for ref in point_refs if ref not in row_lookup]
        if missing_refs:
            topology_rejections[feature["feature_id"]] = "missing_required_geometry_inputs"
            continue

        coords_xyz = [
            (row_lookup[point_ref].easting, row_lookup[point_ref].northing, row_lookup[point_ref].elevation)
            for point_ref in point_refs
        ]
        coords_xy = [(item[0], item[1]) for item in coords_xyz]

        feature_type = feature["feature_type"]
        warnings: list[str] = []
        geometry_type = "Point"
        length_2d: float | None = None
        area_2d: float | None = None

        if feature_type == "point":
            if len(coords_xy) < 1:
                topology_rejections[feature["feature_id"]] = "insufficient_points_for_point"
                continue
            geometry_type = "Point" if len(coords_xy) == 1 else "MultiPoint"

        elif feature_type == "line_string":
            if len(coords_xy) < 2:
                topology_rejections[feature["feature_id"]] = "insufficient_points_for_line_string"
                continue
            geometry_type = "LineString"
            length_2d = _line_length_2d(coords_xy)
            if _line_has_self_intersection(coords_xy):
                topology_rejections[feature["feature_id"]] = "line_self_intersection"
                continue

        elif feature_type == "polygon":
            if len(coords_xy) < 3:
                topology_rejections[feature["feature_id"]] = "insufficient_points_for_polygon"
                continue
            geometry_type = "Polygon"
            ring_xyz = list(coords_xyz)
            ring_xy = list(coords_xy)
            if ring_xyz[0] != ring_xyz[-1]:
                ring_xyz.append(ring_xyz[0])
                ring_xy.append(ring_xy[0])
            coords_xyz = ring_xyz
            coords_xy = ring_xy

            area_2d = _polygon_area_2d(coords_xy)
            if area_2d <= _EPSILON:
                topology_rejections[feature["feature_id"]] = "polygon_zero_area"
                continue
            if _ring_has_self_intersection(coords_xy):
                topology_rejections[feature["feature_id"]] = "polygon_self_intersection"
                continue

        else:
            topology_rejections[feature["feature_id"]] = "unsupported_feature_type"
            continue

        geometry_features.append(
            {
                "feature_id": feature["feature_id"],
                "feature_code": feature["feature_code"],
                "feature_type": feature_type,
                "geometry_type": geometry_type,
                "point_refs": point_refs,
                "coordinates": [
                    {
                        "x": x,
                        "y": y,
                        "z": z,
                    }
                    for x, y, z in coords_xyz
                ],
                "topology": {
                    "is_valid": True,
                    "warnings": warnings,
                    "length_2d": length_2d,
                    "area_2d": area_2d,
                },
            }
        )

    geometry_features.sort(key=lambda item: item["feature_id"])
    return geometry_features, topology_rejections


def _apply_topology_quarantine(
    *,
    intent_features: list[dict[str, Any]],
    routing: dict[str, dict[str, Any]],
    row_lookup: dict[str, BridgeRow],
    topology_rejections: dict[str, str],
) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]]]:
    if not topology_rejections:
        return intent_features, routing

    rejected_feature_ids = set(topology_rejections)
    filtered_features = [item for item in intent_features if item["feature_id"] not in rejected_feature_ids]

    quarantined_records = list(routing["quarantined"]["records"])
    accepted_exact_records = []
    for record in routing["accepted_exact"]["records"]:
        feature_id = record.get("feature_id", "")
        if feature_id in rejected_feature_ids:
            row_ref = str(record.get("row_ref", ""))
            row = row_lookup.get(row_ref)
            quarantined_records.append(
                {
                    "row_ref": row_ref,
                    "point_id": row.point_id if row else record.get("point_id", ""),
                    "reason": "topology_invalid",
                    "detail": topology_rejections.get(feature_id, "topology_invalid"),
                    "candidate_rule_ids": [],
                }
            )
            continue
        accepted_exact_records.append(record)

    accepted_pattern_records = []
    for record in routing["accepted_pattern"]["records"]:
        feature_id = record.get("feature_id", "")
        if feature_id in rejected_feature_ids:
            row_ref = str(record.get("row_ref", ""))
            row = row_lookup.get(row_ref)
            quarantined_records.append(
                {
                    "row_ref": row_ref,
                    "point_id": row.point_id if row else record.get("point_id", ""),
                    "reason": "topology_invalid",
                    "detail": topology_rejections.get(feature_id, "topology_invalid"),
                    "candidate_rule_ids": [],
                }
            )
            continue
        accepted_pattern_records.append(record)

    quarantined_records.sort(key=_sort_route_record)
    accepted_exact_records.sort(key=_sort_route_record)
    accepted_pattern_records.sort(key=_sort_route_record)

    updated_routing = {
        "accepted_exact": {
            "count": len(accepted_exact_records),
            "records": accepted_exact_records,
        },
        "accepted_pattern": {
            "count": len(accepted_pattern_records),
            "records": accepted_pattern_records,
        },
        "quarantined": {
            "count": len(quarantined_records),
            "records": quarantined_records,
        },
    }
    return filtered_features, updated_routing


def _copy_rule_pack_to_run_root(source_rules_path: Path, manifest_dir: Path) -> Path:
    copied_rules_path = manifest_dir / "bridge_rule_pack.yaml"
    copied_rules_path.write_text(source_rules_path.read_text(encoding="utf-8"), encoding="utf-8")
    return copied_rules_path


def run_bridge(
    *,
    run_root: Path,
    rules_path: Path,
    output_manifest_dir: Path | None = None,
) -> BridgeRunResult:
    resolved_run_root = run_root.resolve()
    manifest_dir = output_manifest_dir.resolve() if output_manifest_dir else (resolved_run_root / "manifest")
    manifest_dir.mkdir(parents=True, exist_ok=True)

    points_path = resolved_run_root / "normalized" / "points.csv"
    snapshot_id = _read_snapshot_id(resolved_run_root)
    known_rel_paths = _read_input_rel_paths(resolved_run_root)
    rows = _load_points(points_path, known_rel_paths)

    if not rows:
        raise BridgeConfigError("normalized/points.csv contains no data rows")

    copied_rules_path = _copy_rule_pack_to_run_root(rules_path.resolve(), manifest_dir)
    points_sha256 = _build_points_digest(rows)
    rule_pack_sha256 = _sha256_file(copied_rules_path)
    rule_pack_id, rule_pack_version, precedence, rules_by_type = _load_bridge_rules(copied_rules_path)

    intent_features, routing, row_lookup = _classify_rows(
        rows=rows,
        precedence=precedence,
        rules_by_type=rules_by_type,
    )

    geometry_features, topology_rejections = _build_geometry_features(intent_features, row_lookup)
    intent_features, routing = _apply_topology_quarantine(
        intent_features=intent_features,
        routing=routing,
        row_lookup=row_lookup,
        topology_rejections=topology_rejections,
    )

    filtered_feature_ids = {item["feature_id"] for item in intent_features}
    geometry_features = [item for item in geometry_features if item["feature_id"] in filtered_feature_ids]
    geometry_features.sort(key=lambda item: item["feature_id"])

    unmatched_refs = sorted(
        record["row_ref"]
        for record in routing["quarantined"]["records"]
        if record.get("reason") == "unmatched"
    )
    quarantined_refs = sorted({record["row_ref"] for record in routing["quarantined"]["records"]})

    intent_path = manifest_dir / "intent_ir.json"
    intent_payload = build_contract_payload(
        artifact_type="intent_ir",
        invariants=[
            build_invariant("source_snapshot_hash_bound"),
            build_invariant("rule_pack_version_bound"),
            build_invariant("classification_rule_order_stable"),
            build_invariant(
                "routing_quarantine_reasons_valid",
                passed=all(
                    isinstance(record.get("reason"), str)
                    and record.get("reason") in _QUARANTINE_REASONS
                    for record in routing["quarantined"]["records"]
                ),
                detail="All quarantined records use declared reason taxonomy",
            ),
        ],
        metadata={
            "run_id": resolved_run_root.name,
            "source_snapshot_id": snapshot_id,
            "rule_pack_id": rule_pack_id,
            "rule_pack_version": rule_pack_version,
        },
        paths={
            "run_root": ".",
            "normalized_points_csv": _run_relative(resolved_run_root, points_path),
            "rule_pack": _run_relative(resolved_run_root, copied_rules_path),
        },
        data={
            "points_total": len(rows),
            "mapped_points": routing["accepted_exact"]["count"] + routing["accepted_pattern"]["count"],
            "unmapped_points": len(unmatched_refs),
            "quarantined_points": routing["quarantined"]["count"],
            "features_total": len(intent_features),
            "precedence": list(precedence),
            "features": intent_features,
            "routing": routing,
            "unmapped_point_refs": unmatched_refs,
            "quarantined_point_refs": quarantined_refs,
        },
    )
    write_contract_json(intent_path, intent_payload)
    intent_sha256 = _sha256_json_data(intent_payload["data"])

    geometry_path = manifest_dir / "geometry_ir.json"
    geometry_payload = build_contract_payload(
        artifact_type="geometry_ir",
        invariants=[
            build_invariant("source_snapshot_hash_bound"),
            build_invariant("rule_pack_version_bound"),
            build_invariant("coordinate_resolution_is_source_of_truth"),
            build_invariant("topology_checks_are_deterministic"),
            build_invariant(
                "all_persisted_geometry_features_topology_valid",
                passed=all(item.get("topology", {}).get("is_valid") is True for item in geometry_features),
                detail="Invalid topology features are quarantined before geometry persistence",
            ),
        ],
        metadata={
            "run_id": resolved_run_root.name,
            "source_snapshot_id": snapshot_id,
            "intent_data_sha256": intent_sha256,
        },
        paths={
            "run_root": ".",
            "normalized_points_csv": _run_relative(resolved_run_root, points_path),
            "intent_ir": _run_relative(resolved_run_root, intent_path),
        },
        data={
            "features_total": len(geometry_features),
            "invalid_features": 0,
            "features": geometry_features,
        },
    )
    write_contract_json(geometry_path, geometry_payload)
    geometry_sha256 = _sha256_json_data(geometry_payload["data"])

    bridge_manifest_path = manifest_dir / "bridge_manifest.json"
    bridge_manifest_payload = build_contract_payload(
        artifact_type="bridge_manifest",
        invariants=[
            build_invariant("artifact_hashes_match_payloads"),
            build_invariant("replay_from_manifest_is_possible"),
            build_invariant(
                "routing_fail_closed",
                passed=True,
                detail="Unmatched, ambiguous, and topology-invalid records are quarantined",
            ),
        ],
        metadata={
            "run_id": resolved_run_root.name,
            "source_snapshot_id": snapshot_id,
            "rule_pack_id": rule_pack_id,
            "rule_pack_version": rule_pack_version,
        },
        paths={
            "run_root": ".",
            "normalized_points_csv": _run_relative(resolved_run_root, points_path),
            "dataset_snapshot": _run_relative(resolved_run_root, resolved_run_root / "manifest" / "dataset_snapshot.json"),
            "rule_pack": _run_relative(resolved_run_root, copied_rules_path),
            "intent_ir": _run_relative(resolved_run_root, intent_path),
            "geometry_ir": _run_relative(resolved_run_root, geometry_path),
        },
        data={
            "hashes": {
                "points_sha256": points_sha256,
                "rule_pack_sha256": rule_pack_sha256,
                "intent_data_sha256": intent_sha256,
                "geometry_data_sha256": geometry_sha256,
            },
            "routing": {
                "accepted_exact": routing["accepted_exact"]["count"],
                "accepted_pattern": routing["accepted_pattern"]["count"],
                "quarantined": routing["quarantined"]["count"],
            },
            "counts": {
                "points_total": len(rows),
                "mapped_points": routing["accepted_exact"]["count"] + routing["accepted_pattern"]["count"],
                "unmapped_points": len(unmatched_refs),
                "quarantined_points": routing["quarantined"]["count"],
                "intent_features": len(intent_features),
                "geometry_features": len(geometry_features),
                "invalid_geometry_features": 0,
            },
        },
    )
    write_contract_json(bridge_manifest_path, bridge_manifest_payload)

    return BridgeRunResult(
        run_id=resolved_run_root.name,
        run_root=resolved_run_root,
        intent_path=intent_path,
        geometry_path=geometry_path,
        bridge_manifest_path=bridge_manifest_path,
        mapped_points=routing["accepted_exact"]["count"] + routing["accepted_pattern"]["count"],
        unmapped_points=len(unmatched_refs),
        quarantined_points=routing["quarantined"]["count"],
        intent_features=len(intent_features),
        geometry_features=len(geometry_features),
    )
