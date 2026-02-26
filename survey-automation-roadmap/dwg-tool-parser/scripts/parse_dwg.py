#!/usr/bin/env python3
"""Parse DWG/DXF files into structured JSON with geometry and topology.

DWG parsing requires an external converter command that produces a DXF file.
Pass the converter template via --converter-cmd or DWG_TO_DXF_CMD.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import re
import shlex
import shutil
import subprocess
import sys
import tempfile
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

CONTROL_MARKERS = {
    "SECTION",
    "ENDSEC",
    "EOF",
    "TABLE",
    "ENDTAB",
    "BLOCK",
    "ENDBLK",
    "SEQEND",
}

INSUNITS_MAP = {
    0: "unitless",
    1: "inches",
    2: "feet",
    3: "miles",
    4: "millimeters",
    5: "centimeters",
    6: "meters",
    7: "kilometers",
    8: "microinches",
    9: "mils",
    10: "yards",
    11: "angstroms",
    12: "nanometers",
    13: "microns",
    14: "decimeters",
    15: "decameters",
    16: "hectometers",
    17: "gigameters",
    18: "astronomical_units",
    19: "light_years",
    20: "parsecs",
}

LAYER_CLASS_KEYWORDS = {
    "parcel_boundary": [
        "parcel",
        "boundary",
        "lot",
        "property",
        "prop",
        "tract",
        "plat",
        "row",
        "rightofway",
        "r-o-w",
        "easement",
    ],
    "centerline": [
        "centerline",
        "ctrline",
        "cl",
        "alignment",
        "road",
        "street",
        "lane",
        "baseline",
        "station",
        "chainage",
    ],
    "contour": [
        "contour",
        "cntr",
        "topo",
        "surface",
        "tin",
        "eg",
        "fg",
        "dem",
    ],
    "spot_elevation": [
        "spot",
        "spotelev",
        "elevpt",
        "elevation",
        "gradept",
        "invert",
        "rim",
    ],
    "utility": [
        "utility",
        "util",
        "water",
        "sewer",
        "storm",
        "drain",
        "gas",
        "electric",
        "power",
        "telecom",
        "fiber",
        "duct",
    ],
    "control_point": [
        "control",
        "monument",
        "mon",
        "cp",
        "benchmark",
        "bm",
        "gps",
        "gnss",
        "gis",
        "traverse",
    ],
    "drainage": [
        "drain",
        "ditch",
        "swale",
        "culvert",
        "storm",
        "channel",
    ],
    "construction": [
        "asbuilt",
        "as-built",
        "route",
        "alignment",
        "roadway",
        "bridge",
        "hydro",
        "channel",
        "pipeline",
        "staking",
        "layout",
    ],
    "remote_sensing": [
        "lidar",
        "uav",
        "drone",
        "photogrammetry",
        "bathymetry",
        "sonar",
        "pointcloud",
        "scan",
    ],
}

SURVEY_DOMAIN_KEYWORDS = {
    "boundary_retracement_surveys": [
        "boundary",
        "retrace",
        "retracement",
        "alta",
        "nsps",
        "deed",
        "legal",
        "metes",
        "bounds",
        "closure",
        "monument",
        "corner",
        "property",
        "lotline",
        "rightofway",
        "row",
        "easement",
    ],
    "gps_gis_surveying": [
        "gps",
        "gnss",
        "rtk",
        "ppk",
        "base",
        "rover",
        "geoid",
        "utm",
        "stateplane",
        "state_plane",
        "projection",
        "coordinate",
        "control",
        "gis",
        "geojson",
        "geopackage",
        "shp",
    ],
    "subdivisions_lot_adjustments_easements": [
        "subdivision",
        "subdiv",
        "lot",
        "lotline",
        "lot_adjust",
        "line_adjustment",
        "parcel",
        "block",
        "phase",
        "plat",
        "easement",
        "tract",
        "minor_land_division",
        "boundary_line_adjustment",
    ],
    "site_topography_control_surveys": [
        "topo",
        "topography",
        "topographic",
        "surface",
        "contour",
        "grade",
        "eg",
        "fg",
        "control",
        "benchmark",
        "bm",
        "traverse",
        "site",
        "terrain",
    ],
    "construction_support_surveys": [
        "construction",
        "asbuilt",
        "as_built",
        "stake",
        "staking",
        "layout",
        "route",
        "corridor",
        "roadway",
        "bridge",
        "hydro",
        "channel",
        "alignment",
        "pipeline",
        "utility",
    ],
    "remote_specialized_surveying": [
        "remote",
        "lidar",
        "las",
        "laz",
        "uav",
        "drone",
        "photogrammetry",
        "bathymetry",
        "hydrographic",
        "sonar",
        "multibeam",
        "pointcloud",
        "scan",
        "mobile_mapping",
        "slam",
    ],
}


class ParseError(RuntimeError):
    """Raised when DWG/DXF parsing fails."""


def safe_float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def safe_int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def round_number(value: Any, precision: int) -> float | None:
    number = safe_float(value)
    if number is None:
        return None
    rounded = round(number, precision)
    threshold = 10 ** (-precision)
    if abs(rounded) < threshold:
        return 0.0
    return rounded


def is_point(value: Any) -> bool:
    if not isinstance(value, (list, tuple)):
        return False
    if len(value) < 3:
        return False
    return all(isinstance(coord, (int, float)) for coord in value[:3])


def normalize_point(value: Any, precision: int) -> list[float] | None:
    if value is None:
        return None

    if isinstance(value, (list, tuple)):
        if len(value) == 2:
            value = [value[0], value[1], 0.0]
        elif len(value) >= 3:
            value = [value[0], value[1], value[2]]
        else:
            return None
    elif hasattr(value, "x") and hasattr(value, "y"):
        z_value = getattr(value, "z", 0.0)
        value = [value.x, value.y, z_value]
    else:
        return None

    x = round_number(value[0], precision)
    y = round_number(value[1], precision)
    z = round_number(value[2], precision)
    if x is None or y is None or z is None:
        return None
    return [x, y, z]


def normalize_token(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", value.lower())


def split_tokens(value: str) -> set[str]:
    lowered = value.lower().strip()
    if not lowered:
        return set()

    words = [word for word in re.split(r"[^a-z0-9]+", lowered) if word]
    compact = normalize_token(lowered)

    tokens: set[str] = set(words)
    if compact:
        tokens.add(compact)

    if len(words) >= 2:
        tokens.add("_".join(words))

    return tokens


def collect_context_tokens(values: list[str]) -> set[str]:
    tokens: set[str] = set()
    for value in values:
        tokens.update(split_tokens(value))
    return tokens


def layer_matches_keyword(layer_name: str, keyword: str) -> bool:
    normalized = re.sub(r"[^a-z0-9]+", " ", layer_name.lower()).strip()
    if not normalized:
        return False
    words = normalized.split()
    compact = "".join(words)
    key = re.sub(r"[^a-z0-9]+", "", keyword.lower())
    if not key:
        return False
    if len(key) <= 2:
        return key in words
    return key in normalized or key in compact


def classify_layer(layer_name: str | None) -> set[str]:
    if not layer_name:
        return set()
    classes: set[str] = set()
    for category, keywords in LAYER_CLASS_KEYWORDS.items():
        for keyword in keywords:
            if layer_matches_keyword(layer_name, keyword):
                classes.add(category)
                break
    return classes


def distance_3d(start: list[float], end: list[float]) -> float:
    dx = end[0] - start[0]
    dy = end[1] - start[1]
    dz = end[2] - start[2]
    return math.sqrt(dx * dx + dy * dy + dz * dz)


def polyline_length(vertices: list[list[float]], closed: bool) -> float:
    if len(vertices) < 2:
        return 0.0
    length = 0.0
    for index in range(len(vertices) - 1):
        length += distance_3d(vertices[index], vertices[index + 1])
    if closed:
        length += distance_3d(vertices[-1], vertices[0])
    return length


def polygon_area_xy(vertices: list[list[float]]) -> float:
    if len(vertices) < 3:
        return 0.0
    area = 0.0
    for index in range(len(vertices)):
        x1, y1 = vertices[index][0], vertices[index][1]
        x2, y2 = vertices[(index + 1) % len(vertices)][0], vertices[(index + 1) % len(vertices)][1]
        area += x1 * y2 - x2 * y1
    return abs(area) * 0.5


def extract_spot_elevation_value(text: str) -> float | None:
    compact = text.strip()
    if not compact:
        return None

    # Prefer explicit elevation labels such as "EL=123.45".
    labeled = re.search(r"(?:\bEL\b|\bELEV(?:ATION)?\b|\bZ\b)\s*[:=]?\s*([-+]?\d+(?:\.\d+)?)", compact, re.IGNORECASE)
    if labeled:
        return safe_float(labeled.group(1))

    # Accept plain numeric values commonly used for spot elevation text.
    if re.fullmatch(r"[-+]?\d+(?:\.\d+)?", compact):
        return safe_float(compact)

    return None


def pairs_from_dxf_text(text: str) -> list[tuple[str, str]]:
    lines = text.splitlines()
    pairs: list[tuple[str, str]] = []
    for index in range(0, len(lines) - 1, 2):
        code = lines[index].strip()
        value = lines[index + 1].strip()
        if code:
            pairs.append((code, value))
    return pairs


def extract_header_var(pairs: list[tuple[str, str]], name: str) -> str | None:
    for index, (code, value) in enumerate(pairs[:-1]):
        if code == "9" and value == name:
            return pairs[index + 1][1]
    return None


def consume_attrs_until_next_entity(
    pairs: list[tuple[str, str]],
    start: int,
) -> tuple[list[tuple[str, str]], int]:
    attrs: list[tuple[str, str]] = []
    index = start
    while index < len(pairs):
        code, value = pairs[index]
        if code == "0":
            break
        attrs.append((code, value))
        index += 1
    return attrs, index


def first_attr(attrs: list[tuple[str, str]], code: str) -> str | None:
    for attr_code, attr_value in attrs:
        if attr_code == code:
            return attr_value
    return None


def all_attr(attrs: list[tuple[str, str]], code: str) -> list[str]:
    return [attr_value for attr_code, attr_value in attrs if attr_code == code]


def point_from_attrs(
    attrs: list[tuple[str, str]],
    *,
    x_code: str,
    y_code: str,
    z_code: str = "30",
    default_z: float = 0.0,
    precision: int,
) -> list[float] | None:
    x_raw = first_attr(attrs, x_code)
    y_raw = first_attr(attrs, y_code)
    if x_raw is None or y_raw is None:
        return None
    x = safe_float(x_raw)
    y = safe_float(y_raw)
    z = safe_float(first_attr(attrs, z_code))
    if x is None or y is None:
        return None
    if z is None:
        z = default_z
    return normalize_point([x, y, z], precision)


def combine_text_values(attrs: list[tuple[str, str]], codes: set[str]) -> str | None:
    parts = [value for code, value in attrs if code in codes]
    if not parts:
        return None
    return "".join(parts)


def add_common_attrs(entity: dict[str, Any], attrs: list[tuple[str, str]], precision: int) -> None:
    color = safe_int(first_attr(attrs, "62"))
    if color is not None:
        entity["color"] = color

    true_color = safe_int(first_attr(attrs, "420"))
    if true_color is not None:
        entity["true_color"] = true_color

    linetype = first_attr(attrs, "6")
    if linetype:
        entity["linetype"] = linetype

    lineweight = safe_int(first_attr(attrs, "370"))
    if lineweight is not None:
        entity["lineweight"] = lineweight

    thickness = round_number(first_attr(attrs, "39"), precision)
    if thickness is not None:
        entity["thickness"] = thickness


def normalize_ascii_entity(raw_entity: dict[str, Any], index: int, precision: int) -> dict[str, Any]:
    entity_type = str(raw_entity.get("type", "UNKNOWN"))
    attrs: list[tuple[str, str]] = raw_entity.get("attrs", [])
    entity_id = first_attr(attrs, "5") or f"entity-{index + 1}"

    entity: dict[str, Any] = {
        "id": str(entity_id),
        "type": entity_type,
    }

    layer = first_attr(attrs, "8")
    if layer:
        entity["layer"] = layer

    add_common_attrs(entity, attrs, precision)

    geometry: dict[str, Any] | None = None

    if entity_type == "LINE":
        start = point_from_attrs(attrs, x_code="10", y_code="20", z_code="30", precision=precision)
        end = point_from_attrs(attrs, x_code="11", y_code="21", z_code="31", precision=precision)
        if start and end:
            geometry = {"kind": "line", "start": start, "end": end}

    elif entity_type == "LWPOLYLINE":
        x_values = [safe_float(value) for value in all_attr(attrs, "10")]
        y_values = [safe_float(value) for value in all_attr(attrs, "20")]
        count = min(len(x_values), len(y_values))

        z_raw = first_attr(attrs, "38")
        z_value = safe_float(z_raw)
        if z_value is None:
            z_value = 0.0

        vertices: list[list[float]] = []
        for point_index in range(count):
            x = x_values[point_index]
            y = y_values[point_index]
            if x is None or y is None:
                continue
            point = normalize_point([x, y, z_value], precision)
            if point:
                vertices.append(point)

        flags = safe_int(first_attr(attrs, "70")) or 0
        closed = bool(flags & 1)
        if vertices:
            geometry = {"kind": "polyline", "vertices": vertices, "closed": closed}

            bulges_raw = [safe_float(value) for value in all_attr(attrs, "42")]
            bulges = [
                round_number(value, precision) if value is not None else 0.0
                for value in bulges_raw
            ]
            if bulges and any((value or 0.0) != 0.0 for value in bulges):
                geometry["bulges"] = bulges[: len(vertices)]

    elif entity_type == "POLYLINE":
        vertex_attrs_list: list[list[tuple[str, str]]] = raw_entity.get("vertices", [])
        vertices: list[list[float]] = []
        for vertex_attrs in vertex_attrs_list:
            point = point_from_attrs(
                vertex_attrs,
                x_code="10",
                y_code="20",
                z_code="30",
                precision=precision,
            )
            if point:
                vertices.append(point)

        flags = safe_int(first_attr(attrs, "70")) or 0
        closed = bool(flags & 1)
        if vertices:
            geometry = {"kind": "polyline", "vertices": vertices, "closed": closed}

    elif entity_type == "CIRCLE":
        center = point_from_attrs(attrs, x_code="10", y_code="20", z_code="30", precision=precision)
        radius = round_number(first_attr(attrs, "40"), precision)
        if center and radius is not None:
            geometry = {"kind": "circle", "center": center, "radius": radius}

    elif entity_type == "ARC":
        center = point_from_attrs(attrs, x_code="10", y_code="20", z_code="30", precision=precision)
        radius = safe_float(first_attr(attrs, "40"))
        start_angle = safe_float(first_attr(attrs, "50"))
        end_angle = safe_float(first_attr(attrs, "51"))
        if center and radius is not None and start_angle is not None and end_angle is not None:
            start_radians = math.radians(start_angle)
            end_radians = math.radians(end_angle)
            start = normalize_point(
                [
                    center[0] + radius * math.cos(start_radians),
                    center[1] + radius * math.sin(start_radians),
                    center[2],
                ],
                precision,
            )
            end = normalize_point(
                [
                    center[0] + radius * math.cos(end_radians),
                    center[1] + radius * math.sin(end_radians),
                    center[2],
                ],
                precision,
            )
            geometry = {
                "kind": "arc",
                "center": center,
                "radius": round_number(radius, precision),
                "start_angle": round_number(start_angle, precision),
                "end_angle": round_number(end_angle, precision),
                "start": start,
                "end": end,
            }

    elif entity_type == "POINT":
        point = point_from_attrs(attrs, x_code="10", y_code="20", z_code="30", precision=precision)
        if point:
            geometry = {"kind": "point", "point": point}

    elif entity_type in {"TEXT", "MTEXT"}:
        point = point_from_attrs(attrs, x_code="10", y_code="20", z_code="30", precision=precision)
        text_value = (
            first_attr(attrs, "1")
            if entity_type == "TEXT"
            else combine_text_values(attrs, {"1", "3"})
        )
        if point or text_value:
            geometry = {"kind": "text"}
            if point:
                geometry["point"] = point
            if text_value:
                geometry["text"] = text_value

    elif entity_type == "INSERT":
        insertion_point = point_from_attrs(
            attrs,
            x_code="10",
            y_code="20",
            z_code="30",
            precision=precision,
        )
        block_name = first_attr(attrs, "2")
        x_scale = round_number(first_attr(attrs, "41"), precision)
        y_scale = round_number(first_attr(attrs, "42"), precision)
        z_scale = round_number(first_attr(attrs, "43"), precision)
        rotation = round_number(first_attr(attrs, "50"), precision)
        geometry = {"kind": "insert"}
        if insertion_point:
            geometry["insertion_point"] = insertion_point
        if block_name:
            geometry["block"] = block_name
        geometry["scale"] = [
            x_scale if x_scale is not None else 1.0,
            y_scale if y_scale is not None else 1.0,
            z_scale if z_scale is not None else 1.0,
        ]
        if rotation is not None:
            geometry["rotation_deg"] = rotation

    elif entity_type == "SPLINE":
        x_values = [safe_float(value) for value in all_attr(attrs, "10")]
        y_values = [safe_float(value) for value in all_attr(attrs, "20")]
        z_values = [safe_float(value) for value in all_attr(attrs, "30")]
        count = min(len(x_values), len(y_values))
        points: list[list[float]] = []
        for point_index in range(count):
            x = x_values[point_index]
            y = y_values[point_index]
            z = z_values[point_index] if point_index < len(z_values) and z_values[point_index] is not None else 0.0
            if x is None or y is None:
                continue
            point = normalize_point([x, y, z], precision)
            if point:
                points.append(point)
        if points:
            geometry = {"kind": "spline", "control_points": points}
            if len(points) >= 2:
                geometry["start"] = points[0]
                geometry["end"] = points[-1]

    if geometry:
        entity["geometry"] = geometry

    return entity


def collect_ascii_sections(
    pairs: list[tuple[str, str]],
) -> tuple[set[str], set[str], list[dict[str, Any]]]:
    layers: set[str] = set()
    blocks: set[str] = set()
    raw_entities: list[dict[str, Any]] = []

    current_section: str | None = None
    in_layer_table = False
    index = 0

    while index < len(pairs):
        code, value = pairs[index]

        if code == "0" and value == "SECTION":
            if index + 1 < len(pairs) and pairs[index + 1][0] == "2":
                current_section = pairs[index + 1][1]
                in_layer_table = False
                index += 2
                continue

        if code == "0" and value == "ENDSEC":
            current_section = None
            in_layer_table = False
            index += 1
            continue

        if current_section == "TABLES":
            if (
                code == "0"
                and value == "TABLE"
                and index + 1 < len(pairs)
                and pairs[index + 1][0] == "2"
                and pairs[index + 1][1] == "LAYER"
            ):
                in_layer_table = True
                index += 2
                continue

            if code == "0" and value == "ENDTAB":
                in_layer_table = False

            if in_layer_table and code == "0" and value == "LAYER":
                attrs, next_index = consume_attrs_until_next_entity(pairs, index + 1)
                layer_name = first_attr(attrs, "2")
                if layer_name:
                    layers.add(layer_name)
                index = next_index
                continue

        elif current_section == "BLOCKS":
            if code == "0" and value == "BLOCK":
                attrs, next_index = consume_attrs_until_next_entity(pairs, index + 1)
                block_name = first_attr(attrs, "2")
                if block_name and not block_name.startswith("*"):
                    blocks.add(block_name)
                index = next_index
                continue

        elif current_section == "ENTITIES":
            if code == "0":
                entity_type = value
                if entity_type in {"ENDSEC", "SECTION", "EOF"}:
                    index += 1
                    continue
                if entity_type == "SEQEND":
                    index += 1
                    continue
                if entity_type == "VERTEX":
                    _, next_index = consume_attrs_until_next_entity(pairs, index + 1)
                    index = next_index
                    continue

                if entity_type == "POLYLINE":
                    attrs, cursor = consume_attrs_until_next_entity(pairs, index + 1)
                    vertices: list[list[tuple[str, str]]] = []
                    while cursor < len(pairs):
                        next_code, next_value = pairs[cursor]
                        if next_code != "0":
                            cursor += 1
                            continue
                        if next_value == "VERTEX":
                            vertex_attrs, next_cursor = consume_attrs_until_next_entity(
                                pairs,
                                cursor + 1,
                            )
                            vertices.append(vertex_attrs)
                            cursor = next_cursor
                            continue
                        if next_value == "SEQEND":
                            cursor += 1
                        break

                    raw_entities.append(
                        {
                            "type": entity_type,
                            "attrs": attrs,
                            "vertices": vertices,
                        }
                    )
                    index = cursor
                    continue

                attrs, next_index = consume_attrs_until_next_entity(pairs, index + 1)
                raw_entities.append({"type": entity_type, "attrs": attrs})
                index = next_index
                continue

        index += 1

    return layers, blocks, raw_entities


def parse_ascii_dxf(dxf_path: Path, sample_limit: int, precision: int) -> dict[str, Any]:
    text = dxf_path.read_text(encoding="utf-8", errors="ignore")
    pairs = pairs_from_dxf_text(text)
    if not pairs:
        raise ParseError("DXF file is empty or unreadable as ASCII text.")

    layers, blocks, raw_entities = collect_ascii_sections(pairs)

    entities = [
        normalize_ascii_entity(raw_entity, index, precision)
        for index, raw_entity in enumerate(raw_entities)
    ]
    entity_counts: Counter[str] = Counter(entity["type"] for entity in entities)

    for entity in entities:
        layer_name = entity.get("layer")
        if isinstance(layer_name, str) and layer_name:
            layers.add(layer_name)

    dxf_version = extract_header_var(pairs, "$ACADVER")
    insunits = safe_int(extract_header_var(pairs, "$INSUNITS"))

    summary: dict[str, Any] = {
        "dxf_version": dxf_version,
        "insunits": insunits,
        "entity_total": sum(entity_counts.values()),
        "entity_types": dict(sorted(entity_counts.items())),
        "layers_total": len(layers),
        "layers": sorted(layers),
        "blocks_total": len(blocks),
        "blocks": sorted(blocks),
    }

    return {
        "backend": "ascii-fallback",
        "summary": summary,
        "entities": entities,
        "sample_entities": entities[:sample_limit],
    }


def safe_dxf_get(entity: Any, attr: str) -> Any:
    try:
        if entity.dxf.hasattr(attr):
            return entity.dxf.get(attr)
    except Exception:
        return None
    return None


def bool_attr(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    return False


def normalize_ezdxf_entity(entity: Any, index: int, precision: int) -> dict[str, Any]:
    entity_type = entity.dxftype()
    entity_id = safe_dxf_get(entity, "handle") or f"entity-{index + 1}"
    result: dict[str, Any] = {
        "id": str(entity_id),
        "type": str(entity_type),
    }

    layer = safe_dxf_get(entity, "layer")
    if isinstance(layer, str) and layer:
        result["layer"] = layer

    for attr in ("color", "true_color", "lineweight"):
        raw = safe_dxf_get(entity, attr)
        parsed = safe_int(raw)
        if parsed is not None:
            result[attr] = parsed

    linetype = safe_dxf_get(entity, "linetype")
    if isinstance(linetype, str) and linetype:
        result["linetype"] = linetype

    thickness = round_number(safe_dxf_get(entity, "thickness"), precision)
    if thickness is not None:
        result["thickness"] = thickness

    geometry: dict[str, Any] | None = None

    if entity_type == "LINE":
        start = normalize_point(safe_dxf_get(entity, "start"), precision)
        end = normalize_point(safe_dxf_get(entity, "end"), precision)
        if start and end:
            geometry = {"kind": "line", "start": start, "end": end}

    elif entity_type == "LWPOLYLINE":
        elevation_raw = safe_dxf_get(entity, "elevation")
        elevation = 0.0
        if hasattr(elevation_raw, "z"):
            parsed_z = safe_float(getattr(elevation_raw, "z", None))
            if parsed_z is not None:
                elevation = parsed_z
        else:
            parsed_elevation = safe_float(elevation_raw)
            if parsed_elevation is not None:
                elevation = parsed_elevation

        vertices: list[list[float]] = []
        bulges: list[float] = []
        for point_data in entity.get_points("xyb"):
            x = safe_float(point_data[0]) if len(point_data) > 0 else None
            y = safe_float(point_data[1]) if len(point_data) > 1 else None
            bulge = safe_float(point_data[2]) if len(point_data) > 2 else 0.0
            if x is None or y is None:
                continue
            point = normalize_point([x, y, elevation], precision)
            if point:
                vertices.append(point)
                bulges.append(round_number(bulge, precision) if bulge is not None else 0.0)

        closed = bool_attr(getattr(entity, "closed", False))
        if vertices:
            geometry = {"kind": "polyline", "vertices": vertices, "closed": closed}
            if bulges and any((value or 0.0) != 0.0 for value in bulges):
                geometry["bulges"] = bulges

    elif entity_type == "POLYLINE":
        vertices: list[list[float]] = []
        for vertex in getattr(entity, "vertices", []):
            location = safe_dxf_get(vertex, "location")
            point = normalize_point(location, precision)
            if point:
                vertices.append(point)

        closed = False
        try:
            closed = bool(getattr(entity, "is_closed"))
        except Exception:
            flags = safe_int(safe_dxf_get(entity, "flags")) or 0
            closed = bool(flags & 1)

        if vertices:
            geometry = {"kind": "polyline", "vertices": vertices, "closed": closed}

    elif entity_type == "CIRCLE":
        center = normalize_point(safe_dxf_get(entity, "center"), precision)
        radius = round_number(safe_dxf_get(entity, "radius"), precision)
        if center and radius is not None:
            geometry = {"kind": "circle", "center": center, "radius": radius}

    elif entity_type == "ARC":
        center = normalize_point(safe_dxf_get(entity, "center"), precision)
        radius = safe_float(safe_dxf_get(entity, "radius"))
        start_angle = safe_float(safe_dxf_get(entity, "start_angle"))
        end_angle = safe_float(safe_dxf_get(entity, "end_angle"))

        if center and radius is not None and start_angle is not None and end_angle is not None:
            start_radians = math.radians(start_angle)
            end_radians = math.radians(end_angle)
            start = normalize_point(
                [
                    center[0] + radius * math.cos(start_radians),
                    center[1] + radius * math.sin(start_radians),
                    center[2],
                ],
                precision,
            )
            end = normalize_point(
                [
                    center[0] + radius * math.cos(end_radians),
                    center[1] + radius * math.sin(end_radians),
                    center[2],
                ],
                precision,
            )

            geometry = {
                "kind": "arc",
                "center": center,
                "radius": round_number(radius, precision),
                "start_angle": round_number(start_angle, precision),
                "end_angle": round_number(end_angle, precision),
                "start": start,
                "end": end,
            }

    elif entity_type == "POINT":
        point = normalize_point(safe_dxf_get(entity, "location"), precision)
        if point:
            geometry = {"kind": "point", "point": point}

    elif entity_type == "INSERT":
        insertion = normalize_point(safe_dxf_get(entity, "insert"), precision)
        block_name = safe_dxf_get(entity, "name")
        x_scale = round_number(safe_dxf_get(entity, "xscale"), precision)
        y_scale = round_number(safe_dxf_get(entity, "yscale"), precision)
        z_scale = round_number(safe_dxf_get(entity, "zscale"), precision)
        rotation = round_number(safe_dxf_get(entity, "rotation"), precision)

        geometry = {"kind": "insert"}
        if insertion:
            geometry["insertion_point"] = insertion
        if isinstance(block_name, str) and block_name:
            geometry["block"] = block_name
        geometry["scale"] = [
            x_scale if x_scale is not None else 1.0,
            y_scale if y_scale is not None else 1.0,
            z_scale if z_scale is not None else 1.0,
        ]
        if rotation is not None:
            geometry["rotation_deg"] = rotation

    elif entity_type in {"TEXT", "MTEXT"}:
        insertion = normalize_point(safe_dxf_get(entity, "insert"), precision)
        text_value: str | None = None
        if entity_type == "TEXT":
            text_raw = safe_dxf_get(entity, "text")
            if isinstance(text_raw, str):
                text_value = text_raw
        else:
            try:
                text_value = entity.plain_text()
            except Exception:
                text_raw = safe_dxf_get(entity, "text")
                if isinstance(text_raw, str):
                    text_value = text_raw

        if insertion or text_value:
            geometry = {"kind": "text"}
            if insertion:
                geometry["point"] = insertion
            if text_value:
                geometry["text"] = text_value

    elif entity_type == "SPLINE":
        control_points_raw = []
        try:
            control_points_raw = list(entity.control_points)
        except Exception:
            control_points_raw = []

        if not control_points_raw:
            try:
                control_points_raw = list(entity.fit_points)
            except Exception:
                control_points_raw = []

        control_points = []
        for point in control_points_raw:
            normalized = normalize_point(point, precision)
            if normalized:
                control_points.append(normalized)

        if control_points:
            geometry = {"kind": "spline", "control_points": control_points}
            if len(control_points) >= 2:
                geometry["start"] = control_points[0]
                geometry["end"] = control_points[-1]

    elif entity_type == "ELLIPSE":
        center = normalize_point(safe_dxf_get(entity, "center"), precision)
        major_axis = normalize_point(safe_dxf_get(entity, "major_axis"), precision)
        ratio = round_number(safe_dxf_get(entity, "ratio"), precision)
        start_param = round_number(safe_dxf_get(entity, "start_param"), precision)
        end_param = round_number(safe_dxf_get(entity, "end_param"), precision)
        if center and major_axis and ratio is not None:
            geometry = {
                "kind": "ellipse",
                "center": center,
                "major_axis": major_axis,
                "ratio": ratio,
            }
            if start_param is not None:
                geometry["start_param"] = start_param
            if end_param is not None:
                geometry["end_param"] = end_param

    if geometry:
        result["geometry"] = geometry

    return result


def parse_with_ezdxf(dxf_path: Path, sample_limit: int, precision: int) -> dict[str, Any]:
    try:
        import ezdxf  # type: ignore
    except ImportError as exc:
        raise ParseError("ezdxf is not installed; using fallback parser.") from exc

    try:
        doc = ezdxf.readfile(str(dxf_path))
    except Exception as exc:
        raise ParseError(f"ezdxf could not read DXF: {exc}") from exc

    entities: list[dict[str, Any]] = []
    modelspace = doc.modelspace()
    for index, entity in enumerate(modelspace):
        entities.append(normalize_ezdxf_entity(entity, index, precision))

    entity_counts: Counter[str] = Counter(entity["type"] for entity in entities)

    layer_names: set[str] = set()
    try:
        for layer in doc.layers:
            layer_name = layer.dxf.name
            if layer_name:
                layer_names.add(str(layer_name))
    except Exception:
        for entity in entities:
            layer_name = entity.get("layer")
            if isinstance(layer_name, str) and layer_name:
                layer_names.add(layer_name)

    block_names: set[str] = set()
    try:
        for block in doc.blocks:
            block_name = getattr(block, "name", None)
            if block_name and not str(block_name).startswith("*"):
                block_names.add(str(block_name))
    except Exception:
        pass

    summary: dict[str, Any] = {
        "dxf_version": getattr(doc, "dxfversion", None),
        "insunits": safe_int(doc.header.get("$INSUNITS")),
        "entity_total": sum(entity_counts.values()),
        "entity_types": dict(sorted(entity_counts.items())),
        "layers_total": len(layer_names),
        "layers": sorted(layer_names),
        "blocks_total": len(block_names),
        "blocks": sorted(block_names),
    }

    extmin = normalize_point(doc.header.get("$EXTMIN"), precision)
    extmax = normalize_point(doc.header.get("$EXTMAX"), precision)
    if extmin or extmax:
        summary["extents"] = {"min": extmin, "max": extmax}

    return {
        "backend": "ezdxf",
        "summary": summary,
        "entities": entities,
        "sample_entities": entities[:sample_limit],
    }


def extract_topology_primitives(
    entity: dict[str, Any],
    precision: int,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    geometry = entity.get("geometry")
    if not isinstance(geometry, dict):
        return [], []

    kind = geometry.get("kind")
    if not isinstance(kind, str):
        return [], []

    edges: list[dict[str, Any]] = []
    loops: list[dict[str, Any]] = []

    if kind == "line":
        start = normalize_point(geometry.get("start"), precision)
        end = normalize_point(geometry.get("end"), precision)
        if start and end:
            edges.append({"kind": "segment", "start": start, "end": end})

    elif kind == "polyline":
        vertices_raw = geometry.get("vertices")
        if isinstance(vertices_raw, list):
            vertices = [
                point
                for point in (normalize_point(raw_point, precision) for raw_point in vertices_raw)
                if point is not None
            ]
            for vertex_index in range(len(vertices) - 1):
                edges.append(
                    {
                        "kind": "segment",
                        "start": vertices[vertex_index],
                        "end": vertices[vertex_index + 1],
                    }
                )
            if geometry.get("closed") and len(vertices) >= 2:
                edges.append({"kind": "segment", "start": vertices[-1], "end": vertices[0]})
            if geometry.get("closed") and len(vertices) >= 3:
                loops.append({"kind": "polyline", "vertices": vertices})

    elif kind == "arc":
        start = normalize_point(geometry.get("start"), precision)
        end = normalize_point(geometry.get("end"), precision)
        center = normalize_point(geometry.get("center"), precision)
        radius = round_number(geometry.get("radius"), precision)
        start_angle = round_number(geometry.get("start_angle"), precision)
        end_angle = round_number(geometry.get("end_angle"), precision)

        if start and end:
            edge: dict[str, Any] = {"kind": "arc", "start": start, "end": end}
            if center:
                edge["center"] = center
            if radius is not None:
                edge["radius"] = radius
            if start_angle is not None:
                edge["start_angle"] = start_angle
            if end_angle is not None:
                edge["end_angle"] = end_angle
            edges.append(edge)

    elif kind == "spline":
        start = normalize_point(geometry.get("start"), precision)
        end = normalize_point(geometry.get("end"), precision)
        if not start or not end:
            control_points_raw = geometry.get("control_points")
            if isinstance(control_points_raw, list) and len(control_points_raw) >= 2:
                start = normalize_point(control_points_raw[0], precision)
                end = normalize_point(control_points_raw[-1], precision)
        if start and end:
            edges.append({"kind": "spline", "start": start, "end": end})

    elif kind == "circle":
        center = normalize_point(geometry.get("center"), precision)
        radius = round_number(geometry.get("radius"), precision)
        if center and radius is not None:
            loops.append({"kind": "circle", "center": center, "radius": radius})

    return edges, loops


def geometry_points(geometry: dict[str, Any], precision: int) -> list[list[float]]:
    kind = geometry.get("kind")
    points: list[list[float]] = []

    if kind == "line":
        for key in ("start", "end"):
            point = normalize_point(geometry.get(key), precision)
            if point:
                points.append(point)

    elif kind == "polyline":
        vertices = geometry.get("vertices")
        if isinstance(vertices, list):
            for vertex in vertices:
                point = normalize_point(vertex, precision)
                if point:
                    points.append(point)

    elif kind == "arc":
        for key in ("start", "end", "center"):
            point = normalize_point(geometry.get(key), precision)
            if point:
                points.append(point)

    elif kind == "circle":
        center = normalize_point(geometry.get("center"), precision)
        if center:
            points.append(center)

    elif kind == "point":
        point = normalize_point(geometry.get("point"), precision)
        if point:
            points.append(point)

    elif kind == "text":
        point = normalize_point(geometry.get("point"), precision)
        if point:
            points.append(point)

    elif kind == "insert":
        point = normalize_point(geometry.get("insertion_point"), precision)
        if point:
            points.append(point)

    elif kind == "spline":
        control_points = geometry.get("control_points")
        if isinstance(control_points, list):
            for control_point in control_points:
                point = normalize_point(control_point, precision)
                if point:
                    points.append(point)
        if not points:
            for key in ("start", "end"):
                point = normalize_point(geometry.get(key), precision)
                if point:
                    points.append(point)

    elif kind == "ellipse":
        for key in ("center", "major_axis"):
            point = normalize_point(geometry.get(key), precision)
            if point:
                points.append(point)

    return points


def arc_sweep_degrees(start_angle: float, end_angle: float) -> float:
    raw_delta = end_angle - start_angle
    if abs(raw_delta) >= 360.0:
        return 360.0
    sweep = raw_delta % 360.0
    if sweep == 0.0 and raw_delta != 0.0:
        return 360.0
    return sweep


def estimate_geometry_length(geometry: dict[str, Any], precision: int) -> float | None:
    kind = geometry.get("kind")

    if kind == "line":
        start = normalize_point(geometry.get("start"), precision)
        end = normalize_point(geometry.get("end"), precision)
        if start and end:
            return round_number(distance_3d(start, end), precision)
        return None

    if kind == "polyline":
        vertices_raw = geometry.get("vertices")
        if not isinstance(vertices_raw, list):
            return None
        vertices = [
            point
            for point in (normalize_point(raw_point, precision) for raw_point in vertices_raw)
            if point is not None
        ]
        if len(vertices) < 2:
            return None
        length = polyline_length(vertices, bool(geometry.get("closed")))
        return round_number(length, precision)

    if kind == "arc":
        radius = safe_float(geometry.get("radius"))
        start_angle = safe_float(geometry.get("start_angle"))
        end_angle = safe_float(geometry.get("end_angle"))
        if radius is None or start_angle is None or end_angle is None:
            return None
        sweep = arc_sweep_degrees(start_angle, end_angle)
        length = abs(radius) * math.radians(sweep)
        return round_number(length, precision)

    if kind == "circle":
        radius = safe_float(geometry.get("radius"))
        if radius is None:
            return None
        return round_number(2.0 * math.pi * abs(radius), precision)

    if kind == "spline":
        control_points_raw = geometry.get("control_points")
        if not isinstance(control_points_raw, list):
            return None
        control_points = [
            point
            for point in (normalize_point(raw_point, precision) for raw_point in control_points_raw)
            if point is not None
        ]
        if len(control_points) < 2:
            return None
        length = polyline_length(control_points, False)
        return round_number(length, precision)

    if kind == "ellipse":
        major_axis = normalize_point(geometry.get("major_axis"), precision)
        ratio = safe_float(geometry.get("ratio"))
        if not major_axis or ratio is None:
            return None
        major_radius = math.sqrt(major_axis[0] ** 2 + major_axis[1] ** 2 + major_axis[2] ** 2)
        minor_radius = abs(major_radius * ratio)
        if major_radius == 0 or minor_radius == 0:
            return None
        circumference = 2.0 * math.pi * math.sqrt((major_radius ** 2 + minor_radius ** 2) / 2.0)
        return round_number(circumference, precision)

    return None


def estimate_geometry_area(geometry: dict[str, Any], precision: int) -> float | None:
    kind = geometry.get("kind")

    if kind == "polyline" and geometry.get("closed"):
        vertices_raw = geometry.get("vertices")
        if not isinstance(vertices_raw, list):
            return None
        vertices = [
            point
            for point in (normalize_point(raw_point, precision) for raw_point in vertices_raw)
            if point is not None
        ]
        if len(vertices) < 3:
            return None
        return round_number(polygon_area_xy(vertices), precision)

    if kind == "circle":
        radius = safe_float(geometry.get("radius"))
        if radius is None:
            return None
        return round_number(math.pi * radius * radius, precision)

    return None


def infer_entity_classes(entity: dict[str, Any]) -> set[str]:
    layer = entity.get("layer")
    if isinstance(layer, str):
        return classify_layer(layer)
    return set()


def domain_result(score: int, evidence: list[str]) -> dict[str, Any]:
    if score >= 5:
        confidence = "high"
    elif score >= 2:
        confidence = "medium"
    elif score == 1:
        confidence = "low"
    else:
        confidence = "none"
    return {
        "confidence": confidence,
        "score": score,
        "evidence": evidence,
    }


def matched_domain_keywords(tokens: set[str], domain: str) -> list[str]:
    keywords = SURVEY_DOMAIN_KEYWORDS.get(domain, [])
    matches: list[str] = []
    for keyword in keywords:
        key = normalize_token(keyword)
        if not key:
            continue
        if key in tokens:
            matches.append(keyword)
    return sorted(set(matches))


def build_survey_domain_coverage(
    feature_counts: dict[str, int],
    layer_groups: dict[str, list[str]],
    summary: dict[str, Any],
    topology: dict[str, Any],
    context_tokens: set[str],
) -> dict[str, Any]:
    groups = {name: set(values) for name, values in layer_groups.items()}
    edge_count = safe_int(topology.get("edge_count")) or 0
    loop_count = safe_int(topology.get("loop_count")) or 0
    connected_components = safe_int(topology.get("connected_components")) or 0
    entity_total = safe_int(summary.get("entity_total")) or 0

    boundary_score = 0
    boundary_evidence: list[str] = []
    parcel_count = feature_counts.get("parcel_boundaries", 0)
    if parcel_count > 0:
        boundary_score += min(4, parcel_count)
        boundary_evidence.append(f"{parcel_count} closed parcel boundary candidates detected")
    if "parcel_boundary" in groups:
        boundary_score += 1
        boundary_evidence.append("parcel/boundary layers detected")
    if loop_count > 0:
        boundary_score += 1
        boundary_evidence.append(f"{loop_count} topology loops support boundary/retracement analysis")
    boundary_matches = matched_domain_keywords(context_tokens, "boundary_retracement_surveys")
    if boundary_matches:
        boundary_score += min(2, len(boundary_matches))
        boundary_evidence.append(
            "matched naming conventions: " + ", ".join(boundary_matches[:5])
        )

    gps_gis_score = 0
    gps_gis_evidence: list[str] = []
    control_points = feature_counts.get("control_points", 0)
    spot_points = feature_counts.get("spot_elevation_points", 0)
    if control_points > 0:
        gps_gis_score += min(3, control_points)
        gps_gis_evidence.append(f"{control_points} control point features detected")
    if "control_point" in groups:
        gps_gis_score += 1
        gps_gis_evidence.append("GNSS/GPS/GIS control layer signals present")
    if spot_points > 0:
        gps_gis_score += 1
        gps_gis_evidence.append(f"{spot_points} survey points with elevation values detected")
    if connected_components > 0 and edge_count > 0:
        gps_gis_score += 1
        gps_gis_evidence.append("network topology available for GIS connectivity integration")
    gps_gis_matches = matched_domain_keywords(context_tokens, "gps_gis_surveying")
    if gps_gis_matches:
        gps_gis_score += min(2, len(gps_gis_matches))
        gps_gis_evidence.append(
            "matched naming conventions: " + ", ".join(gps_gis_matches[:5])
        )

    subdivision_score = 0
    subdivision_evidence: list[str] = []
    if parcel_count > 0:
        subdivision_score += min(3, parcel_count)
        subdivision_evidence.append(f"{parcel_count} parcel loop candidates support subdivision workflows")
    if "parcel_boundary" in groups:
        subdivision_score += 1
        subdivision_evidence.append("lot/property/easement style layers detected")
    if feature_counts.get("centerlines", 0) > 0:
        subdivision_score += 1
        subdivision_evidence.append("centerline geometry can support lot-line adjustment coordination")
    subdivision_matches = matched_domain_keywords(
        context_tokens,
        "subdivisions_lot_adjustments_easements",
    )
    if subdivision_matches:
        subdivision_score += min(2, len(subdivision_matches))
        subdivision_evidence.append(
            "matched naming conventions: " + ", ".join(subdivision_matches[:5])
        )

    topo_score = 0
    topo_evidence: list[str] = []
    contours = feature_counts.get("contours", 0)
    spot_labels = feature_counts.get("spot_elevation_labels", 0)
    if contours > 0:
        topo_score += min(4, contours)
        topo_evidence.append(f"{contours} contour entities detected")
    if "contour" in groups:
        topo_score += 1
        topo_evidence.append("topography/surface layers detected")
    if control_points > 0:
        topo_score += 1
        topo_evidence.append("control points support site control surveys")
    if spot_points + spot_labels > 0:
        topo_score += 1
        topo_evidence.append("spot elevations detected for topographic validation")
    topo_matches = matched_domain_keywords(context_tokens, "site_topography_control_surveys")
    if topo_matches:
        topo_score += min(2, len(topo_matches))
        topo_evidence.append("matched naming conventions: " + ", ".join(topo_matches[:5]))

    construction_score = 0
    construction_evidence: list[str] = []
    centerlines = feature_counts.get("centerlines", 0)
    utilities = feature_counts.get("utility_entities", 0)
    if centerlines > 0:
        construction_score += min(3, centerlines)
        construction_evidence.append(f"{centerlines} route/centerline entities detected")
    if utilities > 0:
        construction_score += min(2, utilities)
        construction_evidence.append(f"{utilities} utility/drainage entities detected")
    if "construction" in groups:
        construction_score += 1
        construction_evidence.append("construction support layer naming detected")
    if edge_count > 0:
        construction_score += 1
        construction_evidence.append("topology edges available for as-built/route continuity checks")
    construction_matches = matched_domain_keywords(
        context_tokens,
        "construction_support_surveys",
    )
    if construction_matches:
        construction_score += min(2, len(construction_matches))
        construction_evidence.append(
            "matched naming conventions: " + ", ".join(construction_matches[:5])
        )

    remote_score = 0
    remote_evidence: list[str] = []
    if "remote_sensing" in groups:
        remote_score += 2
        remote_evidence.append("remote sensing layer keywords detected (LiDAR/UAV/bathymetry/etc.)")
    if entity_total >= 5000:
        remote_score += 1
        remote_evidence.append(f"large entity count ({entity_total}) suggests remote/specialized capture datasets")
    if spot_points + contours >= 100:
        remote_score += 1
        remote_evidence.append("dense elevation geometry suggests remote survey support use")
    if control_points > 0 and connected_components > 0:
        remote_score += 1
        remote_evidence.append("control + topology signals support specialized survey georeferencing workflows")
    remote_matches = matched_domain_keywords(context_tokens, "remote_specialized_surveying")
    if remote_matches:
        remote_score += min(2, len(remote_matches))
        remote_evidence.append(
            "matched naming conventions: " + ", ".join(remote_matches[:5])
        )

    return {
        "boundary_retracement_surveys": domain_result(boundary_score, boundary_evidence),
        "gps_gis_surveying": domain_result(gps_gis_score, gps_gis_evidence),
        "subdivisions_lot_adjustments_easements": domain_result(
            subdivision_score,
            subdivision_evidence,
        ),
        "site_topography_control_surveys": domain_result(topo_score, topo_evidence),
        "construction_support_surveys": domain_result(construction_score, construction_evidence),
        "remote_specialized_surveying": domain_result(remote_score, remote_evidence),
    }


def build_civil_survey_summary(
    entities: list[dict[str, Any]],
    topology: dict[str, Any],
    summary: dict[str, Any],
    precision: int,
    sample_limit: int,
    input_path: Path,
    project_tags: list[str],
) -> dict[str, Any]:
    layer_classification: dict[str, list[str]] = {}
    category_layers: dict[str, set[str]] = defaultdict(set)

    for layer_name in summary.get("layers", []):
        if not isinstance(layer_name, str):
            continue
        categories = sorted(classify_layer(layer_name))
        if categories:
            layer_classification[layer_name] = categories
            for category in categories:
                category_layers[category].add(layer_name)

    feature_counts: dict[str, int] = {
        "parcel_boundaries": 0,
        "centerlines": 0,
        "contours": 0,
        "spot_elevation_points": 0,
        "spot_elevation_labels": 0,
        "utility_entities": 0,
        "control_points": 0,
    }

    parcel_candidates: list[dict[str, Any]] = []
    centerline_segments: list[dict[str, Any]] = []
    utility_entity_ids: set[str] = set()
    contour_elevations: list[float] = []
    spot_elevations: list[float] = []
    all_z_values: list[float] = []

    bounds_min: list[float] | None = None
    bounds_max: list[float] | None = None

    centerline_total_length = 0.0

    for entity in entities:
        entity_id = str(entity.get("id", ""))
        entity_type = str(entity.get("type", ""))
        layer = entity.get("layer")
        layer_name = layer if isinstance(layer, str) else None
        classes = infer_entity_classes(entity)

        geometry = entity.get("geometry")
        if not isinstance(geometry, dict):
            continue

        points = geometry_points(geometry, precision)
        for point in points:
            all_z_values.append(point[2])
            if bounds_min is None or bounds_max is None:
                bounds_min = point.copy()
                bounds_max = point.copy()
            else:
                for axis in range(3):
                    bounds_min[axis] = min(bounds_min[axis], point[axis])
                    bounds_max[axis] = max(bounds_max[axis], point[axis])

        length = estimate_geometry_length(geometry, precision)
        area = estimate_geometry_area(geometry, precision)

        metrics: dict[str, Any] = {}
        if length is not None:
            metrics["length"] = length
        if area is not None:
            metrics["area"] = area
        if metrics:
            entity["metrics"] = metrics

        kind = geometry.get("kind")

        if "centerline" in classes and kind in {"line", "polyline", "arc", "spline", "ellipse"}:
            feature_counts["centerlines"] += 1
            if length is not None:
                centerline_total_length += length
                centerline_segments.append(
                    {
                        "entity_id": entity_id,
                        "layer": layer_name,
                        "type": entity_type,
                        "length": length,
                    }
                )

        is_closed_polyline = kind == "polyline" and bool(geometry.get("closed"))
        if "parcel_boundary" in classes and is_closed_polyline:
            feature_counts["parcel_boundaries"] += 1
            parcel: dict[str, Any] = {
                "entity_id": entity_id,
                "layer": layer_name,
            }
            if area is not None:
                parcel["area_sq"] = area
            if length is not None:
                parcel["perimeter"] = length
            parcel_candidates.append(parcel)

        contour_candidate = False
        if "contour" in classes:
            contour_candidate = True
        elif kind in {"line", "polyline", "spline"} and points:
            if any(abs(point[2]) > 0 for point in points):
                contour_candidate = True

        if contour_candidate:
            feature_counts["contours"] += 1
            if points:
                z_values = [point[2] for point in points]
                representative = round_number(sum(z_values) / len(z_values), precision)
                if representative is not None:
                    contour_elevations.append(representative)

        if "utility" in classes or "drainage" in classes:
            feature_counts["utility_entities"] += 1
            utility_entity_ids.add(entity_id)

        if kind == "point" and ("control_point" in classes):
            feature_counts["control_points"] += 1

        if kind == "point" and ("spot_elevation" in classes or "contour" in classes):
            feature_counts["spot_elevation_points"] += 1
            if points:
                spot_elevations.append(points[0][2])

        if kind == "text":
            text_value = geometry.get("text")
            if isinstance(text_value, str):
                parsed = extract_spot_elevation_value(text_value)
                if parsed is not None and (
                    "spot_elevation" in classes
                    or "contour" in classes
                    or "control_point" in classes
                ):
                    feature_counts["spot_elevation_labels"] += 1
                    rounded = round_number(parsed, precision)
                    if rounded is not None:
                        spot_elevations.append(rounded)

    contour_unique = sorted(
        {
            rounded
            for rounded in (round_number(value, precision) for value in contour_elevations)
            if rounded is not None
        }
    )
    contour_interval = None
    if len(contour_unique) >= 2:
        deltas = [
            round_number(contour_unique[index + 1] - contour_unique[index], precision)
            for index in range(len(contour_unique) - 1)
        ]
        positive_deltas = [delta for delta in deltas if delta is not None and delta > 0]
        if positive_deltas:
            delta_counts = Counter(positive_deltas)
            contour_interval = sorted(
                delta_counts.items(),
                key=lambda item: (-item[1], item[0]),
            )[0][0]

    utility_edge_count = 0
    for edge in topology.get("edges", []):
        if not isinstance(edge, dict):
            continue
        if str(edge.get("entity_id", "")) in utility_entity_ids:
            utility_edge_count += 1

    bounds = None
    max_abs_xy = None
    if bounds_min is not None and bounds_max is not None:
        bounds = {"min": bounds_min, "max": bounds_max}
        max_abs_xy = max(abs(bounds_min[0]), abs(bounds_min[1]), abs(bounds_max[0]), abs(bounds_max[1]))

    insunits_code = safe_int(summary.get("insunits"))
    insunits_name = INSUNITS_MAP.get(insunits_code) if insunits_code is not None else None

    coordinate_system_hint = "insufficient geometry for coordinate-system inference"
    if max_abs_xy is not None:
        if max_abs_xy <= 360 and (insunits_code is None or insunits_code == 0):
            coordinate_system_hint = "small coordinate magnitude; verify local grid vs geographic-like values"
        elif max_abs_xy >= 100000 and insunits_name in {"feet", "meters", "kilometers", "yards"}:
            coordinate_system_hint = "magnitude suggests projected survey coordinates (state plane/UTM/local grid)"
        else:
            coordinate_system_hint = "coordinate magnitude appears consistent with local engineering coordinates"

    qa_flags: list[str] = []
    if insunits_code is None or insunits_code == 0:
        qa_flags.append("INSUNITS is missing or unitless; verify drawing units before quantity checks.")
    if feature_counts["parcel_boundaries"] == 0:
        qa_flags.append("No closed parcel boundary candidates were detected.")
    if "contour" in category_layers and feature_counts["contours"] == 0:
        qa_flags.append("Contour-like layers exist but no contour geometry was identified.")
    if topology.get("edge_count", 0) > 0 and topology.get("connected_components", 0) > 1:
        qa_flags.append(
            f"Topology has {topology.get('connected_components')} disconnected components."
        )
    if feature_counts["spot_elevation_points"] == 0 and feature_counts["spot_elevation_labels"] == 0:
        qa_flags.append("No spot elevation points or labels were detected.")
    if contour_unique and len(contour_unique) == 1:
        qa_flags.append("Only one contour elevation level was detected; verify contour interval source.")

    terrain_summary: dict[str, Any] = {
        "contour_count": feature_counts["contours"],
        "contour_elevation_levels": len(contour_unique),
        "contour_interval_estimate": contour_interval,
    }
    if all_z_values:
        terrain_summary["elevation_min"] = round_number(min(all_z_values), precision)
        terrain_summary["elevation_max"] = round_number(max(all_z_values), precision)
        terrain_summary["elevation_span"] = round_number(max(all_z_values) - min(all_z_values), precision)

    parcel_candidates_sorted = sorted(
        parcel_candidates,
        key=lambda parcel: safe_float(parcel.get("area_sq")) or 0.0,
        reverse=True,
    )
    centerline_segments_sorted = sorted(
        centerline_segments,
        key=lambda segment: safe_float(segment.get("length")) or 0.0,
        reverse=True,
    )

    layer_groups = {
        category: sorted(layer_names)
        for category, layer_names in sorted(category_layers.items())
    }
    context_sources: list[str] = []
    context_sources.extend(summary.get("layers", []))
    context_sources.extend(project_tags)
    context_sources.append(input_path.name)
    context_sources.extend([part for part in input_path.parts[-6:]])
    context_tokens = collect_context_tokens([str(source) for source in context_sources])

    survey_domains = build_survey_domain_coverage(
        feature_counts=feature_counts,
        layer_groups=layer_groups,
        summary=summary,
        topology=topology,
        context_tokens=context_tokens,
    )

    return {
        "units": {
            "insunits_code": insunits_code,
            "insunits_name": insunits_name,
        },
        "bounds": bounds,
        "coordinate_system_hint": coordinate_system_hint,
        "layer_classification": layer_classification,
        "layer_groups": layer_groups,
        "project_context": {
            "input_file_name": input_path.name,
            "project_tags": project_tags,
            "context_token_count": len(context_tokens),
            "sample_tokens": sorted(context_tokens)[:40],
        },
        "feature_counts": feature_counts,
        "terrain": terrain_summary,
        "parcels": {
            "count": feature_counts["parcel_boundaries"],
            "total_area_sq": round_number(
                sum(safe_float(parcel.get("area_sq")) or 0.0 for parcel in parcel_candidates),
                precision,
            ),
            "samples": parcel_candidates_sorted[:sample_limit],
        },
        "centerlines": {
            "count": feature_counts["centerlines"],
            "total_length": round_number(centerline_total_length, precision),
            "samples": centerline_segments_sorted[:sample_limit],
        },
        "utilities": {
            "entity_count": feature_counts["utility_entities"],
            "topology_edge_count": utility_edge_count,
        },
        "spot_elevations": {
            "point_count": feature_counts["spot_elevation_points"],
            "label_count": feature_counts["spot_elevation_labels"],
            "min": round_number(min(spot_elevations), precision) if spot_elevations else None,
            "max": round_number(max(spot_elevations), precision) if spot_elevations else None,
            "samples": [
                round_number(value, precision)
                for value in sorted(spot_elevations)[:sample_limit]
            ],
        },
        "survey_domains": survey_domains,
        "qa_flags": qa_flags,
    }


def build_topology(entities: list[dict[str, Any]], tolerance: float, precision: int) -> dict[str, Any]:
    if tolerance <= 0:
        raise ParseError("tolerance must be > 0.")

    node_lookup: dict[tuple[int, int, int], str] = {}
    nodes: list[dict[str, Any]] = []
    edges: list[dict[str, Any]] = []
    loops: list[dict[str, Any]] = []
    neighbors: dict[str, set[str]] = defaultdict(set)

    def point_key(point: list[float]) -> tuple[int, int, int]:
        return (
            int(round(point[0] / tolerance)),
            int(round(point[1] / tolerance)),
            int(round(point[2] / tolerance)),
        )

    def ensure_node(point: list[float]) -> str:
        normalized = normalize_point(point, precision)
        if not normalized:
            raise ParseError("Invalid topology point encountered during node creation.")
        key = point_key(normalized)
        existing = node_lookup.get(key)
        if existing:
            return existing
        node_id = f"n{len(nodes) + 1}"
        node_lookup[key] = node_id
        nodes.append({"id": node_id, "point": normalized})
        return node_id

    for entity in entities:
        entity_id = str(entity.get("id", ""))
        edge_defs, loop_defs = extract_topology_primitives(entity, precision)

        for edge_def in edge_defs:
            start = normalize_point(edge_def.get("start"), precision)
            end = normalize_point(edge_def.get("end"), precision)
            if not start or not end:
                continue

            start_node = ensure_node(start)
            end_node = ensure_node(end)
            edge_id = f"e{len(edges) + 1}"

            edge_record: dict[str, Any] = {
                "id": edge_id,
                "entity_id": entity_id,
                "kind": edge_def.get("kind", "segment"),
                "start_node": start_node,
                "end_node": end_node,
            }

            geometry: dict[str, Any] = {}
            center = normalize_point(edge_def.get("center"), precision)
            if center:
                geometry["center"] = center
            for numeric_key in ("radius", "start_angle", "end_angle"):
                numeric_value = round_number(edge_def.get(numeric_key), precision)
                if numeric_value is not None:
                    geometry[numeric_key] = numeric_value
            if geometry:
                edge_record["geometry"] = geometry

            edges.append(edge_record)
            neighbors[start_node].add(end_node)
            neighbors[end_node].add(start_node)

        for loop_def in loop_defs:
            loop_id = f"l{len(loops) + 1}"
            loop_kind = str(loop_def.get("kind", "loop"))
            loop_record: dict[str, Any] = {
                "id": loop_id,
                "entity_id": entity_id,
                "kind": loop_kind,
            }

            if loop_kind == "polyline":
                vertices_raw = loop_def.get("vertices")
                if isinstance(vertices_raw, list):
                    node_ids: list[str] = []
                    for raw_vertex in vertices_raw:
                        vertex = normalize_point(raw_vertex, precision)
                        if vertex:
                            node_ids.append(ensure_node(vertex))
                    if node_ids:
                        if node_ids[0] != node_ids[-1]:
                            node_ids.append(node_ids[0])
                        loop_record["nodes"] = node_ids

            if loop_kind == "circle":
                center = normalize_point(loop_def.get("center"), precision)
                radius = round_number(loop_def.get("radius"), precision)
                if center:
                    loop_record["center"] = center
                if radius is not None:
                    loop_record["radius"] = radius

            loops.append(loop_record)

    node_ids = [node["id"] for node in nodes]
    visited: set[str] = set()
    connected_components = 0

    for node_id in node_ids:
        if node_id in visited:
            continue
        connected_components += 1
        stack = [node_id]
        while stack:
            current = stack.pop()
            if current in visited:
                continue
            visited.add(current)
            for neighbor in neighbors.get(current, set()):
                if neighbor not in visited:
                    stack.append(neighbor)

    adjacency = {
        node_id: sorted(neighbor_nodes)
        for node_id, neighbor_nodes in neighbors.items()
        if neighbor_nodes
    }

    junction_nodes = sum(1 for node_id in node_ids if len(neighbors.get(node_id, set())) not in {0, 2})

    return {
        "tolerance": tolerance,
        "node_count": len(nodes),
        "edge_count": len(edges),
        "loop_count": len(loops),
        "connected_components": connected_components,
        "junction_nodes": junction_nodes,
        "nodes": nodes,
        "edges": edges,
        "loops": loops,
        "adjacency": adjacency,
    }


def build_converter_command(
    template: str,
    input_path: Path,
    output_path: Path,
) -> tuple[list[str], str]:
    if "{input}" not in template or "{output}" not in template:
        raise ParseError("Converter template must include {input} and {output}.")

    substitutions = {
        "input": str(input_path),
        "output": str(output_path),
        "output_dir": str(output_path.parent),
        "output_stem": output_path.stem,
    }
    try:
        command_text = template.format(**substitutions)
    except KeyError as exc:
        missing = str(exc).strip("'")
        raise ParseError(f"Unknown converter placeholder: {{{missing}}}") from exc

    try:
        command_parts = shlex.split(command_text)
    except ValueError as exc:
        raise ParseError(f"Invalid converter command: {exc}") from exc

    if not command_parts:
        raise ParseError("Converter command resolved to an empty command.")

    return command_parts, command_text


def convert_dwg_to_dxf(
    input_path: Path,
    converter_template: str,
) -> tuple[Path, Path, str]:
    temp_dir = Path(tempfile.mkdtemp(prefix="dwg-parser-"))
    output_path = temp_dir / f"{input_path.stem}.dxf"

    command_parts, command_text = build_converter_command(
        converter_template,
        input_path,
        output_path,
    )

    completed = subprocess.run(
        command_parts,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        stderr = completed.stderr.strip() or "(no stderr)"
        raise ParseError(f"Converter command failed (exit {completed.returncode}): {stderr}")

    if not output_path.exists():
        raise ParseError(
            f"Converter command completed but output DXF was not created: {output_path}"
        )

    return output_path, temp_dir, command_text


def parse_input(
    input_path: Path,
    converter_template: str | None,
    sample_limit: int,
    precision: int,
) -> tuple[dict[str, Any], dict[str, Any], Path | None]:
    suffix = input_path.suffix.lower()
    temp_dir: Path | None = None
    conversion_info: dict[str, Any] = {"used": False}
    dxf_path = input_path

    if suffix == ".dwg":
        if not converter_template:
            raise ParseError(
                "DWG input requires a converter command. Set --converter-cmd or DWG_TO_DXF_CMD."
            )
        dxf_path, temp_dir, resolved_command = convert_dwg_to_dxf(
            input_path=input_path,
            converter_template=converter_template,
        )
        conversion_info = {
            "used": True,
            "command": resolved_command,
            "output_dxf": str(dxf_path),
        }
    elif suffix != ".dxf":
        raise ParseError("Unsupported input type. Use a .dwg or .dxf file.")

    backend_notes: list[str] = []
    try:
        result = parse_with_ezdxf(dxf_path, sample_limit, precision)
    except ParseError as exc:
        backend_notes.append(str(exc))
        result = parse_ascii_dxf(dxf_path, sample_limit, precision)

    if backend_notes:
        result["notes"] = backend_notes

    return result, conversion_info, temp_dir


def write_report(report: dict[str, Any], output_path: Path | None) -> None:
    serialized = json.dumps(report, indent=2, sort_keys=True)
    if output_path is None:
        print(serialized)
        return
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(serialized + "\n", encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Parse DWG/DXF files into a JSON summary with CAD geometry and topology.",
    )
    parser.add_argument("input_file", help="Path to input .dwg or .dxf file")
    parser.add_argument("--output", help="Path to JSON output file")
    parser.add_argument(
        "--converter-cmd",
        help=(
            "DWG->DXF converter template. Must include {input} and {output}. "
            "Optional placeholders: {output_dir}, {output_stem}."
        ),
    )
    parser.add_argument(
        "--sample-limit",
        type=int,
        default=25,
        help="Max number of sample entities to include (default: 25).",
    )
    parser.add_argument(
        "--precision",
        type=int,
        default=6,
        help="Decimal precision for output coordinates (default: 6).",
    )
    parser.add_argument(
        "--tolerance",
        type=float,
        default=1e-6,
        help="Point merge tolerance for topology graph (default: 1e-6).",
    )
    parser.add_argument(
        "--entity-limit",
        type=int,
        default=2000,
        help="Max number of full entities to emit in report (default: 2000).",
    )
    parser.add_argument(
        "--topology-entity-limit",
        type=int,
        default=5000,
        help="Max number of entities used to build topology (default: 5000).",
    )
    parser.add_argument(
        "--project-tag",
        action="append",
        default=[],
        help=(
            "Project context tag for domain tuning. "
            "Repeat this flag for multiple tags, e.g. --project-tag roadway --project-tag retracement."
        ),
    )
    parser.add_argument(
        "--keep-temp",
        action="store_true",
        help="Keep temporary converted DXF files when parsing DWG inputs.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    input_path = Path(args.input_file).expanduser().resolve()
    output_path = Path(args.output).expanduser().resolve() if args.output else None

    if args.sample_limit < 1:
        print("sample-limit must be >= 1", file=sys.stderr)
        return 2
    if args.precision < 0:
        print("precision must be >= 0", file=sys.stderr)
        return 2
    if args.tolerance <= 0:
        print("tolerance must be > 0", file=sys.stderr)
        return 2
    if args.entity_limit < 1:
        print("entity-limit must be >= 1", file=sys.stderr)
        return 2
    if args.topology_entity_limit < 1:
        print("topology-entity-limit must be >= 1", file=sys.stderr)
        return 2
    if not input_path.exists():
        print(f"Input file not found: {input_path}", file=sys.stderr)
        return 2

    converter_template = args.converter_cmd or os.getenv("DWG_TO_DXF_CMD")
    temp_dir: Path | None = None

    try:
        parse_result, conversion_info, temp_dir = parse_input(
            input_path=input_path,
            converter_template=converter_template,
            sample_limit=args.sample_limit,
            precision=args.precision,
        )

        all_entities: list[dict[str, Any]] = parse_result["entities"]
        emitted_entities = all_entities[: args.entity_limit]
        topology_source = all_entities[: args.topology_entity_limit]
        topology = build_topology(
            entities=topology_source,
            tolerance=args.tolerance,
            precision=args.precision,
        )
        topology["source_entities"] = len(topology_source)
        topology["source_truncated"] = len(topology_source) < len(all_entities)

        summary = dict(parse_result["summary"])
        summary["geometry_entities_total"] = sum(
            1 for entity in all_entities if isinstance(entity.get("geometry"), dict)
        )
        civil_survey = build_civil_survey_summary(
            entities=all_entities,
            topology=topology,
            summary=summary,
            precision=args.precision,
            sample_limit=args.sample_limit,
            input_path=input_path,
            project_tags=args.project_tag,
        )
        if len(emitted_entities) < len(all_entities):
            civil_survey["qa_flags"].append(
                "Entity list is truncated in report; full civil analysis still used all parsed entities."
            )
        if len(topology_source) < len(all_entities):
            civil_survey["qa_flags"].append(
                "Topology source entities were truncated; network metrics may be partial."
            )

        report: dict[str, Any] = {
            "input_file": str(input_path),
            "input_type": input_path.suffix.lower().lstrip("."),
            "parsed_at_utc": datetime.now(timezone.utc).isoformat(),
            "parser_backend": parse_result["backend"],
            "summary": summary,
            "sample_entities": parse_result["sample_entities"],
            "entities_total": len(all_entities),
            "entities_returned": len(emitted_entities),
            "entities_truncated": len(emitted_entities) < len(all_entities),
            "entities": emitted_entities,
            "topology": topology,
            "civil_survey": civil_survey,
        }
        if conversion_info.get("used"):
            report["conversion"] = conversion_info
        if "notes" in parse_result:
            report["notes"] = parse_result["notes"]

        write_report(report, output_path)
        return 0
    except ParseError as exc:
        print(f"Parse error: {exc}", file=sys.stderr)
        return 1
    finally:
        if temp_dir is not None and not args.keep_temp:
            shutil.rmtree(temp_dir, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())
