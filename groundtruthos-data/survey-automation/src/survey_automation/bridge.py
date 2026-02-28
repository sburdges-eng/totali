from __future__ import annotations

import json
import hashlib
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, List, Dict
from dataclasses import asdict

from .bridge_models import (
    BRIDGE_SCHEMA_VERSION,
    IntentArtifact,
    IntentFeature,
    GeometryArtifact,
    GeometryFeature,
    BridgeManifest,
)
from .models import PointRecord

class IntentBridge:
    def __init__(self, run_root: Path):
        self.run_root = run_root
        self.points_path = run_root / "normalized/points.csv"
        self.points: List[PointRecord] = []
        self.intent_artifact: IntentArtifact | None = None
        self.geometry_artifact: GeometryArtifact | None = None

    def bind_source(self):
        if not self.points_path.exists():
            raise FileNotFoundError(f"Source points not found: {self.points_path}")

        self.points.clear()

        # Simple CSV loading for now (assuming standard format)
        import csv
        with open(self.points_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                self.points.append(PointRecord(
                    point_id=row["point_id"],
                    northing=float(row["northing"]),
                    easting=float(row["easting"]),
                    elevation=float(row["elevation"]),
                    description=row["description"],
                    dwg_description=row.get("dwg_description", ""),
                    dwg_layer=row.get("dwg_layer", ""),
                    locked=row.get("locked", ""),
                    group_name=row.get("group_name", ""),
                    category=row.get("category", ""),
                    ls_number=row.get("ls_number", ""),
                    source_file=row.get("source_file", ""),
                    source_line=int(row.get("source_line") or 0)
                ))

    def derive_intent(self, rules: List[Any]):
        # Placeholder for rule-based intent derivation
        # For now, group by description code as a simple heuristic
        grouped_features: Dict[str, List[PointRecord]] = {}
        for p in self.points:
            code = p.description.split()[0] if p.description else "UNKNOWN"
            if code not in grouped_features:
                grouped_features[code] = []
            grouped_features[code].append(p)

        features = []
        for code, points in grouped_features.items():
            features.append(IntentFeature(
                feature_id=f"feat-{code}",
                feature_type="line", # Simplified assumption
                group_name=code,
                source_point_ids=[p.point_id for p in points]
            ))

        self.intent_artifact = IntentArtifact(
            schemaVersion=BRIDGE_SCHEMA_VERSION,
            artifactType="intent_ir",
            invariants=["paths_are_relative", "deterministic_key_order"],
            metadata={
                "run_id": self.run_root.name,
                "generated_at": datetime.now(timezone.utc).isoformat()
            },
            paths={"source_points": "normalized/points.csv"},
            data={"features": features}
        )

    def derive_geometry(self):
        if not self.intent_artifact:
            raise ValueError("Intent artifact not derived")

        geom_features = []
        feature_map = {f.feature_id: f for f in self.intent_artifact.data["features"]}
        point_map = {p.point_id: p for p in self.points}

        for feat_id, feat in feature_map.items():
            coords = []
            for pid in feat.source_point_ids:
                pt = point_map.get(pid)
                if pt:
                    coords.append([pt.easting, pt.northing, pt.elevation])

            geom_features.append(GeometryFeature(
                feature_id=f"geom-{feat_id}",
                geometry_type=feat.feature_type,
                coordinates=coords,
                intent_feature_id=feat_id
            ))

        self.geometry_artifact = GeometryArtifact(
            schemaVersion=BRIDGE_SCHEMA_VERSION,
            artifactType="geometry_ir",
            invariants=["paths_are_relative", "deterministic_key_order"],
            metadata={
                "run_id": self.run_root.name,
                "generated_at": datetime.now(timezone.utc).isoformat()
            },
            paths={"intent_artifact": "intent_ir.json"},
            data={"features": geom_features}
        )

    def export(self, output_dir: Path):
        output_dir.mkdir(parents=True, exist_ok=True)


        if self.intent_artifact:
            with open(output_dir / "intent_ir.json", "w") as f:
                json.dump(asdict(self.intent_artifact), f, indent=2, sort_keys=True)

        if self.geometry_artifact:
            with open(output_dir / "geometry_ir.json", "w") as f:
                json.dump(asdict(self.geometry_artifact), f, indent=2, sort_keys=True)

        # Bridge Manifest
        manifest = BridgeManifest(
            schemaVersion=BRIDGE_SCHEMA_VERSION,
            artifactType="bridge_manifest",
            invariants=["paths_are_relative", "deterministic_key_order"],
            metadata={
                "run_id": self.run_root.name,
                "generated_at": datetime.now(timezone.utc).isoformat()
            },
            paths={
                "intent_artifact": "intent_ir.json",
                "geometry_artifact": "geometry_ir.json"
            },
            data={}
        )
        with open(output_dir / "bridge_manifest.json", "w") as f:
            json.dump(asdict(manifest), f, indent=2, sort_keys=True)
