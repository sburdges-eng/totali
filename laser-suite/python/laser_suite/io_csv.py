from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path

from .schemas import AdjacencyPair, GeometryRecord, Observation, SetbackRecord, Station, WeightRule


class BundleError(ValueError):
    pass


@dataclass(slots=True)
class CanonicalBundle:
    stations: list[Station]
    observations: list[Observation]
    weights: list[WeightRule]
    adjacency: list[AdjacencyPair]
    boundaries: list[GeometryRecord]
    improvements: list[GeometryRecord]
    easements: list[GeometryRecord]
    setbacks: list[SetbackRecord]


_BUNDLE_FILES = {
    "stations": "stations.csv",
    "observations": "observations.csv",
    "weights": "weights.csv",
    "adjacency": "adjacency.csv",
    "boundaries": "boundaries.csv",
    "improvements": "improvements.csv",
    "easements": "easements.csv",
    "setbacks": "setbacks.csv",
}


_REQUIRED_COLUMNS = {
    "stations": ["station_id", "x", "y", "z", "status"],
    "observations": ["obs_id", "from_stn", "to_stn", "type", "value"],
    "weights": ["obs_type", "std_dev", "ppm"],
    "adjacency": ["station_i", "station_j"],
    "boundaries": ["boundary_id", "wkt_geometry"],
    "improvements": ["imp_id", "wkt_geometry"],
    "easements": ["easement_id", "wkt_geometry"],
    "setbacks": ["setback_id", "boundary_id", "distance_m"],
}


def _read_rows(path: Path, required_columns: list[str]) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        fieldnames = reader.fieldnames or []
        missing = [col for col in required_columns if col not in fieldnames]
        if missing:
            raise BundleError(f"{path.name} missing required columns: {', '.join(missing)}")
        rows: list[dict[str, str]] = []
        for row in reader:
            normalized = {key: (value.strip() if isinstance(value, str) else "") for key, value in row.items()}
            rows.append(normalized)
        return rows


def _float(value: str, context: str) -> float:
    try:
        return float(value)
    except ValueError as exc:
        raise BundleError(f"Invalid float for {context}: {value}") from exc


def load_bundle(bundle_dir: Path) -> CanonicalBundle:
    missing = [name for name in _BUNDLE_FILES.values() if not (bundle_dir / name).exists()]
    if missing:
        raise BundleError("Missing bundle files: " + ", ".join(missing))

    stations_rows = _read_rows(bundle_dir / _BUNDLE_FILES["stations"], _REQUIRED_COLUMNS["stations"])
    observations_rows = _read_rows(bundle_dir / _BUNDLE_FILES["observations"], _REQUIRED_COLUMNS["observations"])
    weights_rows = _read_rows(bundle_dir / _BUNDLE_FILES["weights"], _REQUIRED_COLUMNS["weights"])
    adjacency_rows = _read_rows(bundle_dir / _BUNDLE_FILES["adjacency"], _REQUIRED_COLUMNS["adjacency"])
    boundaries_rows = _read_rows(bundle_dir / _BUNDLE_FILES["boundaries"], _REQUIRED_COLUMNS["boundaries"])
    improvements_rows = _read_rows(bundle_dir / _BUNDLE_FILES["improvements"], _REQUIRED_COLUMNS["improvements"])
    easements_rows = _read_rows(bundle_dir / _BUNDLE_FILES["easements"], _REQUIRED_COLUMNS["easements"])
    setbacks_rows = _read_rows(bundle_dir / _BUNDLE_FILES["setbacks"], _REQUIRED_COLUMNS["setbacks"])

    stations = [
        Station(
            station_id=row["station_id"],
            x=_float(row["x"], f"stations.csv:{row['station_id']}:x"),
            y=_float(row["y"], f"stations.csv:{row['station_id']}:y"),
            z=_float(row["z"], f"stations.csv:{row['station_id']}:z"),
            status=row["status"].lower(),
        )
        for row in stations_rows
    ]

    observations = [
        Observation(
            obs_id=row["obs_id"],
            from_stn=row["from_stn"],
            to_stn=row["to_stn"],
            obs_type=row["type"].lower(),
            value=_float(row["value"], f"observations.csv:{row['obs_id']}:value"),
            sigma_override=_float(row["sigma"], f"observations.csv:{row['obs_id']}:sigma")
            if row.get("sigma")
            else None,
        )
        for row in observations_rows
    ]

    weights = [
        WeightRule(
            obs_type=row["obs_type"].lower(),
            std_dev=_float(row["std_dev"], f"weights.csv:{row['obs_type']}:std_dev"),
            ppm=_float(row["ppm"], f"weights.csv:{row['obs_type']}:ppm"),
        )
        for row in weights_rows
    ]

    adjacency = [AdjacencyPair(station_i=row["station_i"], station_j=row["station_j"]) for row in adjacency_rows]
    boundaries = [GeometryRecord(record_id=row["boundary_id"], wkt_geometry=row["wkt_geometry"]) for row in boundaries_rows]
    improvements = [GeometryRecord(record_id=row["imp_id"], wkt_geometry=row["wkt_geometry"]) for row in improvements_rows]
    easements = [GeometryRecord(record_id=row["easement_id"], wkt_geometry=row["wkt_geometry"]) for row in easements_rows]
    setbacks = [
        SetbackRecord(
            setback_id=row["setback_id"],
            boundary_id=row["boundary_id"],
            distance_m=_float(row["distance_m"], f"setbacks.csv:{row['setback_id']}:distance_m"),
        )
        for row in setbacks_rows
    ]

    station_ids = {station.station_id for station in stations}
    for obs in observations:
        if obs.from_stn not in station_ids or obs.to_stn not in station_ids:
            raise BundleError(f"Observation {obs.obs_id} references unknown station")
    for pair in adjacency:
        if pair.station_i not in station_ids or pair.station_j not in station_ids:
            raise BundleError(f"Adjacency pair references unknown station: {pair.station_i}, {pair.station_j}")

    return CanonicalBundle(
        stations=stations,
        observations=observations,
        weights=weights,
        adjacency=adjacency,
        boundaries=boundaries,
        improvements=improvements,
        easements=easements,
        setbacks=setbacks,
    )
