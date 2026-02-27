"""
Phase 1: Geodetic Gatekeeper
=============================
Deterministic ETL – enforces CRS/epoch/unit metadata at ingestion.
Rejects ambiguous inputs. Applies PROJ-based transformations.
"""

import hashlib
import json
import uuid
from pathlib import Path
from typing import Optional

import numpy as np
import laspy
from pyproj import CRS, Transformer
from pyproj.exceptions import CRSError

from totali.pipeline.models import (
    PhaseResult, CRSMetadata, PointCloudStats
)
from totali.pipeline.base_phase import PipelinePhase
from totali.pipeline.context import PipelineContext
from totali.audit.logger import AuditLogger
from totali.geodetic.crs_inference import (
    CRSInferenceEngine,
    INTERNATIONAL_FOOT_TO_METER,
    US_SURVEY_FOOT_TO_METER,
)

try:
    from totali.quarantine_ui.app import add_to_quarantine
except Exception:  # pragma: no cover - optional dependency (Flask)
    add_to_quarantine = None


class GeodeticGatekeeper(PipelinePhase):
    phase_name = "geodetic"

    def __init__(self, config: dict, audit: AuditLogger):
        super().__init__(config, audit)
        self.allowed_crs = [CRS.from_user_input(c) for c in config.get("allowed_crs", [])]
        self.allowed_epsg = [c.to_epsg() for c in self.allowed_crs]
        self.reject_mixed_datum = config.get("reject_on_mixed_datum", True)
        self.reject_missing_crs = config.get("reject_on_missing_crs", True)
        self.geoid_model = config.get("geoid_model", "GEOID18")
        self.crs_inference_cfg = config.get("crs_inference", {})
        self.crs_inference_enabled = self.crs_inference_cfg.get("enabled", not self.reject_missing_crs)
        self.crs_confidence_threshold = float(
            self.crs_inference_cfg.get("confidence_threshold", 0.8)
        )
        self.auto_assign_high_confidence = self.crs_inference_cfg.get(
            "auto_assign_high_confidence", True
        )
        self.internal_metric_standardization = config.get(
            "internal_metric_standardization", False
        )

    def validate_inputs(self, context: PipelineContext) -> tuple[bool, list[str]]:
        errors: list[str] = []
        if not context.input_path:
            errors.append("input_path is required")
        elif not Path(context.input_path).exists():
            errors.append(f"Input path does not exist: {context.input_path}")
        if not self.allowed_epsg:
            errors.append("allowed_crs must contain at least one EPSG CRS")
        elif self.allowed_epsg[0] is None:
            errors.append("allowed_crs contains non-EPSG CRS; expected EPSG-backed entries")
        return len(errors) == 0, errors

    def run(self, context: PipelineContext) -> PhaseResult:
        input_path = Path(context.input_path)
        output_dir = Path(context.output_dir)

        # Read point cloud
        las = laspy.read(str(input_path))
        if len(las.points) == 0:
            return PhaseResult(
                phase="geodetic",
                success=False,
                message="Input point cloud is empty",
            )

        # Extract and validate CRS
        crs_meta = self._extract_crs(las, input_path)

        if not crs_meta.is_valid:
            return PhaseResult(
                phase="geodetic",
                success=False,
                message=f"CRS validation failed: {crs_meta.validation_errors}",
            )

        # Compute stats
        stats = self._compute_stats(las, input_path, crs_meta)

        # Hash input for chain of custody
        input_hash = self._hash_file(input_path)
        self.audit.log("ingest", {
            "file": str(input_path),
            "sha256": input_hash,
            "point_count": stats.point_count,
            "crs": f"EPSG:{crs_meta.epsg_code}",
            "bounds_min": stats.bounds_min.tolist() if stats.bounds_min is not None else None,
            "bounds_max": stats.bounds_max.tolist() if stats.bounds_max is not None else None,
        })

        # Transform if needed
        points_xyz, transform_applied = self._apply_transforms(las, crs_meta)

        if transform_applied:
            self.audit.log("transform", {
                "from_crs": f"EPSG:{crs_meta.epsg_code}",
                "to_crs": f"EPSG:{self.allowed_epsg[0]}",
                "geoid": self.geoid_model,
            })

        # Write standardized output
        out_path = output_dir / f"{input_path.stem}_gated.las"
        self._write_output(las, points_xyz, out_path, crs_meta)

        # Metadata report
        report_path = output_dir / f"{input_path.stem}_geodetic_report.json"
        report = {
            "input_file": str(input_path),
            "input_hash": input_hash,
            "crs": {
                "epsg": crs_meta.epsg_code,
                "epoch": crs_meta.epoch,
                "geoid": crs_meta.geoid_model,
                "h_unit": crs_meta.horizontal_unit,
                "v_unit": crs_meta.vertical_unit,
            },
            "point_count": stats.point_count,
            "bounds": {
                "min": stats.bounds_min.tolist() if stats.bounds_min is not None else None,
                "max": stats.bounds_max.tolist() if stats.bounds_max is not None else None,
            },
            "transform_applied": transform_applied,
            "validation_passed": True,
        }
        with open(report_path, "w") as f:
            json.dump(report, f, indent=2)

        return PhaseResult(
            phase="geodetic",
            success=True,
            message="CRS validated, data standardized",
            data={
                "points_xyz": points_xyz,
                "las": las,
                "crs": crs_meta,
                "stats": stats,
                "input_hash": input_hash,
            },
            output_files=[out_path, report_path],
        )

    def _extract_crs(self, las: laspy.LasData, path: Path) -> CRSMetadata:
        meta = CRSMetadata(epsg_code=0)
        errors: list[str] = []

        # Try to get CRS from LAS VLRs
        crs_wkt = None
        for vlr in las.vlrs:
            if vlr.record_id == 2112:  # OGC WKT
                crs_wkt = vlr.record_data.decode("utf-8", errors="ignore").strip("\x00")
                break

        if crs_wkt:
            try:
                crs = CRS.from_wkt(crs_wkt)
                epsg = crs.to_epsg()
                if epsg:
                    meta.epsg_code = epsg
                    meta.source_datum = crs.datum.name if crs.datum else None
                else:
                    errors.append("CRS found but no EPSG code resolvable")
            except CRSError as e:
                errors.append(f"Invalid CRS WKT: {e}")
        # Fallback: use laspy header CRS if present
        elif hasattr(las, "header") and hasattr(las.header, "parse_crs"):
            try:
                crs = las.header.parse_crs()
                if crs is not None:
                    epsg = crs.to_epsg()
                    if epsg:
                        meta.epsg_code = epsg
                        meta.source_datum = crs.datum.name if getattr(crs, "datum", None) else None
                    else:
                        errors.append("CRS found in LAS header but no EPSG code resolvable")
                elif self.reject_missing_crs:
                    errors.append("No CRS metadata found in LAS file")
            except Exception as e:
                errors.append(f"Failed to parse CRS from LAS header: {e}")
        else:
            if self.reject_missing_crs:
                errors.append("No CRS metadata found in LAS file")

        # Spatial heuristic fallback when metadata is missing/unknown.
        if (
            meta.epsg_code in (None, 0)
            and not self.reject_missing_crs
            and self.crs_inference_enabled
        ):
            xyz = np.column_stack([las.x, las.y, las.z])
            bounds_min = xyz.min(axis=0)
            bounds_max = xyz.max(axis=0)

            engine = CRSInferenceEngine()
            candidates = engine.infer_from_bounds(bounds_min, bounds_max)
            inferred_unit, _ = engine.infer_unit_scale(bounds_min, bounds_max)
            if inferred_unit != "unknown":
                meta.horizontal_unit = inferred_unit
                meta.vertical_unit = inferred_unit

            needs_review = engine.requires_human_review(
                candidates,
                confidence_threshold=self.crs_confidence_threshold,
            )
            if needs_review:
                if candidates and add_to_quarantine is not None:
                    item_id = uuid.uuid4().hex[:8]
                    add_to_quarantine(
                        item_id=item_id,
                        filename=path.name,
                        point_count=len(las.points),
                        bounds_min=bounds_min.tolist(),
                        bounds_max=bounds_max.tolist(),
                        candidates=candidates,
                        output_dir=str(path.parent),
                    )
                    errors.append(
                        f"CRS ambiguous, queued for human review (ID: {item_id})"
                    )
                else:
                    errors.append("No CRS metadata found and inference failed")
            elif candidates and self.auto_assign_high_confidence:
                meta.epsg_code = candidates[0].epsg
                meta.horizontal_unit = candidates[0].unit
                meta.vertical_unit = candidates[0].unit
                self.audit.log(
                    "crs_inferred",
                    {
                        "file": str(path),
                        "inferred_epsg": meta.epsg_code,
                        "confidence": candidates[0].confidence,
                        "method": "spatial_heuristic",
                    },
                )
            else:
                errors.append("No CRS metadata found and inference failed")

        # Validate against allowed list
        if meta.epsg_code and meta.epsg_code not in self.allowed_epsg:
            errors.append(
                f"EPSG:{meta.epsg_code} not in allowed CRS list: {self.allowed_epsg}"
            )
        if meta.epsg_code in (None, 0):
            errors.append(
                "Invalid or missing EPSG code (EPSG:0). Please provide a valid CRS."
            )

        meta.geoid_model = self.geoid_model
        meta.horizontal_unit = meta.horizontal_unit or self.config.get(
            "elevation_unit", "US_survey_foot"
        )
        meta.vertical_unit = meta.vertical_unit or self.config.get(
            "elevation_unit", "US_survey_foot"
        )
        meta.epoch = self.config.get("required_epoch")
        meta.validation_errors = errors
        meta.is_valid = len(errors) == 0

        return meta

    def _compute_stats(
        self, las: laspy.LasData, path: Path, crs: CRSMetadata
    ) -> PointCloudStats:
        xyz = np.column_stack([las.x, las.y, las.z])
        return PointCloudStats(
            point_count=len(las.points),
            bounds_min=xyz.min(axis=0),
            bounds_max=xyz.max(axis=0),
            has_rgb=hasattr(las, "red"),
            has_intensity=hasattr(las, "intensity"),
            has_classification=hasattr(las, "classification"),
            source_file=str(path),
            crs=crs,
        )

    def _apply_transforms(
        self, las: laspy.LasData, crs: CRSMetadata
    ) -> tuple[np.ndarray, bool]:
        xyz = np.column_stack([las.x, las.y, las.z])

        # If EPSG code is unknown/missing, no transform can be applied
        if not crs.epsg_code:
            return self._standardize_units(xyz, crs), False

        # If CRS matches first allowed CRS, no transform needed
        if crs.epsg_code == self.allowed_epsg[0]:
            return self._standardize_units(xyz, crs), False

        if crs.epsg_code in (None, 0):
            raise ValueError(
                "Invalid EPSG code detected (EPSG:0). Please provide a valid CRS."
            )

        # Apply PROJ transformation
        source_crs = CRS.from_epsg(crs.epsg_code)
        target_crs = CRS.from_epsg(self.allowed_epsg[0])
        transformer = Transformer.from_crs(source_crs, target_crs, always_xy=True)

        x_out, y_out, z_out = transformer.transform(xyz[:, 0], xyz[:, 1], xyz[:, 2])
        transformed = np.column_stack([x_out, y_out, z_out])
        return self._standardize_units(transformed, crs), True

    def _standardize_units(self, xyz: np.ndarray, crs: CRSMetadata) -> np.ndarray:
        if not self.internal_metric_standardization:
            return xyz
        unit = (crs.horizontal_unit or "").lower()
        if unit in {"meter", "metre", "m"}:
            return xyz
        if unit in {"us_survey_foot", "us survey foot", "ftus"}:
            factor = US_SURVEY_FOOT_TO_METER
        elif unit in {"foot", "feet", "ft", "international_foot"}:
            factor = INTERNATIONAL_FOOT_TO_METER
        else:
            return xyz
        crs.horizontal_unit = "meter"
        crs.vertical_unit = "meter"
        return xyz * factor

    def _write_output(
        self, las: laspy.LasData, xyz: np.ndarray, out_path: Path, crs: CRSMetadata
    ):
        header = laspy.LasHeader(point_format=las.header.point_format, version="1.4")
        header.offsets = xyz.min(axis=0)
        header.scales = [0.001, 0.001, 0.001]

        out_las = laspy.LasData(header)
        out_las.x = xyz[:, 0]
        out_las.y = xyz[:, 1]
        out_las.z = xyz[:, 2]

        # Copy classification if present
        if hasattr(las, "classification"):
            out_las.classification = las.classification
        if hasattr(las, "intensity"):
            out_las.intensity = las.intensity

        out_las.write(str(out_path))

    def _hash_file(self, path: Path) -> str:
        h = hashlib.sha256()
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                h.update(chunk)
        return h.hexdigest()
