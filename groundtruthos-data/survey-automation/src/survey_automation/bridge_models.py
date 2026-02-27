from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, List

BRIDGE_SCHEMA_VERSION = "1.0.0"

@dataclass(slots=True)
class IntentFeature:
    feature_id: str
    feature_type: str
    group_name: str
    source_point_ids: List[str]
    metadata: dict[str, Any] = field(default_factory=dict)

@dataclass(slots=True)
class IntentArtifact:
    schemaVersion: str
    artifactType: str
    invariants: List[str]
    metadata: dict[str, Any]
    paths: dict[str, str]
    data: dict[str, Any]  # features: List[IntentFeature]

@dataclass(slots=True)
class GeometryFeature:
    feature_id: str
    geometry_type: str
    coordinates: List[List[float]]
    intent_feature_id: str

@dataclass(slots=True)
class GeometryArtifact:
    schemaVersion: str
    artifactType: str
    invariants: List[str]
    metadata: dict[str, Any]
    paths: dict[str, str]
    data: dict[str, Any]  # features: List[GeometryFeature]

@dataclass(slots=True)
class BridgeManifest:
    schemaVersion: str
    artifactType: str
    invariants: List[str]
    metadata: dict[str, Any]
    paths: dict[str, str]
    data: dict[str, Any]  # hash_catalog, invariant_results
