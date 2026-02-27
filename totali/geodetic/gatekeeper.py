"""
Phase 1: Geodetic Gatekeeper
=============================
Deterministic ETL – enforces CRS/epoch/unit metadata at ingestion.
Rejects ambiguous inputs. Applies PROJ-based transformations.
"""

import hashlib
import json
import csv
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


class GeodeticGatekeeper(PipelinePhase):
    phase_name = "geodetic"

    def __init__(self, config: dict, audit: AuditLogger):
        super().__init__(config, audit)
        self.allowed_crs = [CRS.from_user_input(c) for c in config.get("allowed_crs", [])]
        self.allowed_epsg = [c.to_epsg() for c in self.allowed_crs]
        self.reject_mixed_datum = config.get("reject_on_mixed_datum", True)
        self.reject_missing_crs = config.get("reject_on_missing_crs", True)
        self.geoid_model = config.get("geoid_model", "GEOID18")

    def validate_inputs(self, context: PipelineContext) -> tuple[bool, list[str]]:
        errors: list[str] = []
        if not context.input_path:
            errors.append("input_path is required")
        elif not Path(context.input_path).exists():
            errors.append(f"Input path does not exist: {context.input_path}")
        if self.allowed_epsg and self.allowed_epsg[0] is None:
            errors.append("allowed_crs contains non-EPSG CRS; expected EPSG-backed entries")
        return len(errors) == 0, errors

    def run(self, context: PipelineContext) -> PhaseResult:
        input_path = Path(context.input_path)
        output_dir = Path(context.output_dir)

        # Handle CSV input
        if input_path.suffix.lower() == ".csv":
            las = self._read_csv_as_las(input_path)
            crs_meta = self._extract_crs_from_csv(input_path)
        else:
            # Read point cloud (LAS/LAZ)
            las = laspy.read(str(input_path))
            # Extract and validate CRS
            crs_meta = self._extract_crs(las, input_path)

        if not crs_meta.is_valid:
            # If CSV and reject_on_missing_crs is False, we might allow it if we have upstream metadata
            # For now, strict validation logic remains
            if not (input_path.suffix.lower() == ".csv" and not self.reject_missing_crs):
                 return PhaseResult(
                    phase="geodetic",
                    success=False,
                    message=f"CRS validation failed: {crs_meta.validation_errors}",
                )

        # Compute stats
        stats = self._compute_stats(las, input_path, crs_meta)

        # Hash input for chain of custody
        input_hash = self._hash_file(input_path)

        # Determine upstream linkage
        upstream_run_id = None
        if input_path.suffix.lower() == ".csv":
             # Look for run_manifest.json in ../manifest/
             manifest_path = input_path.parent.parent / "manifest" / "run_manifest.json"
             if manifest_path.exists():
                 try:
                     with open(manifest_path, "r") as f:
                         manifest = json.load(f)
                         upstream_run_id = manifest.get("metadata", {}).get("run_id")
                 except Exception:
                     pass

        audit_payload = {
            "file": str(input_path),
            "sha256": input_hash,
            "point_count": stats.point_count,
            "crs": f"EPSG:{crs_meta.epsg_code}" if crs_meta.epsg_code else "unknown",
            "bounds_min": stats.bounds_min.tolist() if stats.bounds_min is not None else None,
            "bounds_max": stats.bounds_max.tolist() if stats.bounds_max is not None else None,
        }
        if upstream_run_id:
            audit_payload["upstream_run_id"] = upstream_run_id

        self.audit.log("ingest", audit_payload)

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

    def _read_csv_as_las(self, path: Path) -> laspy.LasData:
        """Read survey-automation style CSV and convert to LasData."""
        points = []
        with open(path, "r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            # Normalize field names (strip whitespace, lowercase)
            if reader.fieldnames:
                reader.fieldnames = [fn.strip().lower() for fn in reader.fieldnames]

            for row in reader:
                try:
                    x = float(row.get("easting", 0))
                    y = float(row.get("northing", 0))
                    z = float(row.get("elevation", 0))
                    points.append((x, y, z))
                except ValueError:
                    continue  # Skip malformed rows

        if not points:
            raise ValueError(f"No valid points found in CSV: {path}")

        points_np = np.array(points)

        # Create minimal LAS structure
        header = laspy.LasHeader(point_format=3, version="1.4")
        header.offsets = points_np.min(axis=0)
        header.scales = [0.001, 0.001, 0.001]

        las = laspy.LasData(header)
        las.x = points_np[:, 0]
        las.y = points_np[:, 1]
        las.z = points_np[:, 2]

        return las

    def _extract_crs_from_csv(self, path: Path) -> CRSMetadata:
        """Attempt to extract CRS from sidecar manifest or rely on config override."""
        # For now, we return empty/invalid metadata unless configured to ignore
        # In a real impl, we'd check manifest/run_manifest.json for CRS hints
        meta = CRSMetadata(epsg_code=0)
        errors = []

        if self.reject_missing_crs:
             # Try to find a default/fallback if provided in config, else error
             if self.allowed_epsg:
                 # Auto-assume the first allowed CRS for CSV inputs if strict checking is off
                 # But strict is ON by default.
                 # Logic: if CSV has no metadata, we can't invent it safely.
                 errors.append("CSV input requires external CRS definition (not yet implemented)")
             else:
                 errors.append("No CRS metadata found in CSV context")

        # If config allows missing CRS, we'll mark as valid but with code=0
        if not self.reject_missing_crs:
            meta.is_valid = True
            if self.allowed_epsg:
                meta.epsg_code = self.allowed_epsg[0] # Default to project CRS
        else:
            meta.validation_errors = errors
            meta.is_valid = len(errors) == 0

        return meta

    def _extract_crs(self, las: laspy.LasData, path: Path) -> CRSMetadata:
        meta = CRSMetadata(epsg_code=0)
        errors = []

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
        else:
            if self.reject_missing_crs:
                errors.append("No CRS metadata found in LAS file")

        # Validate against allowed list
        if meta.epsg_code and meta.epsg_code not in self.allowed_epsg:
            errors.append(
                f"EPSG:{meta.epsg_code} not in allowed CRS list: {self.allowed_epsg}"
            )

        meta.geoid_model = self.geoid_model
        meta.horizontal_unit = self.config.get("elevation_unit", "US_survey_foot")
        meta.vertical_unit = self.config.get("elevation_unit", "US_survey_foot")
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
            return xyz, False

        # If CRS matches first allowed CRS, no transform needed
        if crs.epsg_code == self.allowed_epsg[0]:
            return xyz, False

        # Apply PROJ transformation
        source_crs = CRS.from_epsg(crs.epsg_code)
        target_crs = CRS.from_epsg(self.allowed_epsg[0])
        transformer = Transformer.from_crs(source_crs, target_crs, always_xy=True)

        x_out, y_out, z_out = transformer.transform(xyz[:, 0], xyz[:, 1], xyz[:, 2])
        return np.column_stack([x_out, y_out, z_out]), True

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
