"""
Phase: Survey Integrator
=========================
Loads external survey data (geometry artifacts) and control networks (adjusted stations).
Bridges the gap between field survey and LiDAR drafting.
"""

import csv
import json
from pathlib import Path
import numpy as np

from totali.pipeline.base_phase import PipelinePhase
from totali.pipeline.context import PipelineContext
from totali.pipeline.models import PhaseResult, SurveyData, SurveyFeature
from totali.audit.logger import AuditLogger


class SurveyIntegrator(PipelinePhase):
    phase_name = "integration"

    def __init__(self, config: dict, audit: AuditLogger):
        super().__init__(config, audit)
        self.geometry_ir_path = config.get("geometry_ir_path")
        self.control_csv_path = config.get("control_csv_path")

    def run(self, context: PipelineContext) -> PhaseResult:
        survey_data = SurveyData()
        loaded_files = []

        # Load survey geometry from Survey Automation
        if self.geometry_ir_path:
            path = Path(self.geometry_ir_path)
            if path.exists():
                features = self._load_geometry_ir(path)
                survey_data.features.extend(features)
                loaded_files.append(str(path))
                self.audit.log("integration_load", {
                    "source": "geometry_ir",
                    "path": str(path),
                    "feature_count": len(features),
                })
            else:
                self.audit.log("integration_warn", {
                    "message": f"Geometry IR path not found: {path}"
                })

        # Load control points from Laser Suite
        if self.control_csv_path:
            path = Path(self.control_csv_path)
            if path.exists():
                control = self._load_control_csv(path)
                survey_data.control_points.extend(control)
                loaded_files.append(str(path))
                self.audit.log("integration_load", {
                    "source": "control_csv",
                    "path": str(path),
                    "point_count": len(control),
                })
            else:
                self.audit.log("integration_warn", {
                    "message": f"Control CSV path not found: {path}"
                })

        survey_data.source_files = loaded_files

        return PhaseResult(
            phase="integration",
            success=True,
            message=f"Loaded {len(survey_data.features)} features and {len(survey_data.control_points)} control points",
            data={"survey_data": survey_data},
        )

    def _load_geometry_ir(self, path: Path) -> list[SurveyFeature]:
        """Parse geometry_ir.json from Survey Automation."""
        features = []
        try:
            with open(path, "r") as f:
                payload = json.load(f)

            # Navigate to data.features
            items = payload.get("data", {}).get("features", [])

            for item in items:
                coords = item.get("coordinates", [])
                if not coords:
                    continue

                # Convert coordinates to numpy array
                pts = []
                for c in coords:
                    x = c.get("x", 0.0)
                    y = c.get("y", 0.0)
                    z = c.get("z", 0.0)
                    pts.append([x, y, z])
                pts_np = np.array(pts)

                feat = SurveyFeature(
                    feature_id=item.get("feature_id", "unknown"),
                    feature_code=item.get("feature_code", "unknown"),
                    feature_type=item.get("geometry_type", "Unknown"),
                    geometry=pts_np,
                    attributes={
                        "point_refs": item.get("point_refs", []),
                        "topology_valid": item.get("topology", {}).get("is_valid", False),
                    }
                )
                features.append(feat)

        except Exception as e:
            self.audit.log("integration_error", {"error": str(e), "file": str(path)})
            # We don't raise here to allow partial success, or maybe we should?
            # PipelinePhase usually catches exceptions in orchestrator.
            # But let's raise to fail fast if config is bad.
            raise

        return features

    def _load_control_csv(self, path: Path) -> list[SurveyFeature]:
        """Parse stations_adjusted.csv from Laser Suite."""
        control_points = []
        try:
            with open(path, "r", newline="") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    sid = row.get("station_id", "unknown")
                    try:
                        x = float(row.get("x", 0.0))
                        y = float(row.get("y", 0.0))
                        z_val = row.get("z", "").strip()
                        z = float(z_val) if z_val else 0.0
                    except ValueError:
                        self.audit.log("integration_warn", {"message": f"Invalid coordinates for station {sid}"})
                        continue

                    feat = SurveyFeature(
                        feature_id=sid,
                        feature_code="CONTROL",
                        feature_type="Point",
                        geometry=np.array([[x, y, z]]),
                        attributes={"station_id": sid},
                        source_layer="TOTaLi-SURV-CONTROL"
                    )
                    control_points.append(feat)

        except Exception as e:
            self.audit.log("integration_error", {"error": str(e), "file": str(path)})
            raise

        return control_points
