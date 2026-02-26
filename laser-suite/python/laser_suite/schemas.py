from __future__ import annotations

from dataclasses import dataclass

SCHEMA_VERSION = "1.0.0"


@dataclass(slots=True)
class Station:
    station_id: str
    x: float
    y: float
    z: float
    status: str


@dataclass(slots=True)
class Observation:
    obs_id: str
    from_stn: str
    to_stn: str
    obs_type: str
    value: float
    sigma_override: float | None


@dataclass(slots=True)
class WeightRule:
    obs_type: str
    std_dev: float
    ppm: float


@dataclass(slots=True)
class AdjacencyPair:
    station_i: str
    station_j: str


@dataclass(slots=True)
class GeometryRecord:
    record_id: str
    wkt_geometry: str


@dataclass(slots=True)
class SetbackRecord:
    setback_id: str
    boundary_id: str
    distance_m: float
