from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from shapely import set_precision
from shapely.geometry import LineString, MultiLineString, MultiPolygon, Point, Polygon
from shapely.ops import unary_union
from shapely.wkt import loads as load_wkt

from .io_csv import CanonicalBundle


@dataclass(slots=True)
class EncroachmentRow:
    item_id: str
    condition_type: str
    location_reference: str
    magnitude: float
    units: str
    status: str


def _normalize_geom(wkt_geometry: str, tol: float):
    geom = load_wkt(wkt_geometry)
    geom = set_precision(geom, grid_size=tol)
    if not geom.is_valid:
        geom = geom.buffer(0)
    return geom


def _iter_polygons(geom):
    if isinstance(geom, Polygon):
        yield geom
    elif isinstance(geom, MultiPolygon):
        for item in geom.geoms:
            yield item


def _max_vertex_distance(from_geom, to_geom) -> float:
    max_dist = 0.0
    for poly in _iter_polygons(from_geom):
        for x, y in poly.exterior.coords:
            dist = Point(x, y).distance(to_geom)
            if dist > max_dist:
                max_dist = dist
    return float(max_dist)


def _crossing_length(a, b) -> float:
    cross = a.boundary.intersection(b.boundary)
    if isinstance(cross, (LineString, MultiLineString)):
        return float(cross.length)
    return 0.0


def analyze_encroachments(bundle: CanonicalBundle, snap_tolerance_m: float) -> dict[str, Any]:
    boundaries = {item.record_id: _normalize_geom(item.wkt_geometry, snap_tolerance_m) for item in bundle.boundaries}
    improvements = {item.record_id: _normalize_geom(item.wkt_geometry, snap_tolerance_m) for item in bundle.improvements}
    easements = {item.record_id: _normalize_geom(item.wkt_geometry, snap_tolerance_m) for item in bundle.easements}

    rows: list[EncroachmentRow] = []
    row_num = 1

    for imp_id, imp_geom in sorted(improvements.items()):
        for bnd_id, bnd_geom in sorted(boundaries.items()):
            outside = imp_geom.difference(bnd_geom)
            if not outside.is_empty and outside.area > (snap_tolerance_m * snap_tolerance_m):
                length = _crossing_length(imp_geom, bnd_geom)
                depth = _max_vertex_distance(outside, bnd_geom.boundary)
                rows.append(
                    EncroachmentRow(
                        item_id=f"E-{row_num:03d}",
                        condition_type="Boundary Crossing",
                        location_reference=f"improvement={imp_id}; boundary={bnd_id}",
                        magnitude=depth if depth > 0 else length,
                        units="m",
                        status="potential_encroachment",
                    )
                )
                row_num += 1

        for eas_id, eas_geom in sorted(easements.items()):
            intr = imp_geom.intersection(eas_geom)
            if not intr.is_empty and intr.area > (snap_tolerance_m * snap_tolerance_m):
                rows.append(
                    EncroachmentRow(
                        item_id=f"E-{row_num:03d}",
                        condition_type="Easement Intrusion",
                        location_reference=f"improvement={imp_id}; easement={eas_id}",
                        magnitude=float(intr.area),
                        units="m2",
                        status="potential_encroachment",
                    )
                )
                row_num += 1

    for setback in sorted(bundle.setbacks, key=lambda item: item.setback_id):
        if setback.boundary_id not in boundaries:
            continue
        boundary = boundaries[setback.boundary_id]
        setback_area = boundary.buffer(-setback.distance_m)
        if setback_area.is_empty:
            continue

        for imp_id, imp_geom in sorted(improvements.items()):
            violation = imp_geom.difference(setback_area)
            if not violation.is_empty and violation.area > (snap_tolerance_m * snap_tolerance_m):
                rows.append(
                    EncroachmentRow(
                        item_id=f"E-{row_num:03d}",
                        condition_type="Setback Violation",
                        location_reference=f"improvement={imp_id}; boundary={setback.boundary_id}; setback={setback.setback_id}",
                        magnitude=float(violation.area),
                        units="m2",
                        status="potential_violation",
                    )
                )
                row_num += 1

    compliant = len(rows) == 0
    return {
        "rows": rows,
        "compliant": compliant,
        "row_count": len(rows),
    }
