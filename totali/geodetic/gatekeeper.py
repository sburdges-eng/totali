"""
Phase 1: Geodetic Gatekeeper
=============================
Deterministic ETL – enforces CRS/epoch/unit metadata at ingestion.
Rejects ambiguous inputs. Applies PROJ-based transformations.
"""

import hashlib
import json
from pathlib import Path

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

    # G-3: canonical unit alias sets. Case-insensitive, whitespace-normalized.
    _US_SURVEY_FOOT_ALIASES = frozenset(
        {"us_survey_foot", "us survey foot", "ftus", "usft"}
    )
    _METRIC_ALIASES = frozenset({"meter", "metre", "m", "meters", "metres"})
    _INTERNATIONAL_FOOT_ALIASES = frozenset(
        {"foot", "feet", "ft", "international_foot"}
    )

    def __init__(self, config: dict, audit: AuditLogger):
        super().__init__(config, audit)
        self.allowed_crs = [CRS.from_user_input(c) for c in config.get("allowed_crs", [])]
        self.allowed_epsg = [c.to_epsg() for c in self.allowed_crs]
        self.reject_mixed_datum = config.get("reject_on_mixed_datum", True)
        self.reject_missing_crs = config.get("reject_on_missing_crs", True)
        self.geoid_model = config.get("geoid_model", "GEOID18")
        # G-3: tolerance carried into unit_rejected audit payloads for
        # downstream elevation-precision checks.
        self.unit_tolerance_ft = float(config.get("unit_tolerance_ft", 0.01))

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

        # Read point cloud
        las = laspy.read(str(input_path))

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

        # G-3: unit validation after defaults. Categorical — no silent coercion.
        errors.extend(self._validate_units(meta, path))

        meta.validation_errors = errors
        meta.is_valid = len(errors) == 0

        return meta

    @staticmethod
    def _normalize_unit(unit):
        return (unit or "").strip().lower()

    def _validate_units(self, crs: CRSMetadata, path: Path) -> list[str]:
        """G-3: US-survey-foot categorical enforcement. No silent coercion.

        Metric → `_reject_metric` (reason=metric_not_allowed).
        Non-canonical label → `unit_rejected` (reason=unit_mismatch).
        Pass → `unit_validated`.
        """
        errors: list[str] = []
        canonical = self._normalize_unit(
            self.config.get("elevation_unit", "US_survey_foot")
        )
        h_unit = self._normalize_unit(crs.horizontal_unit)
        v_unit = self._normalize_unit(crs.vertical_unit)

        if h_unit in self._METRIC_ALIASES or v_unit in self._METRIC_ALIASES:
            self._reject_metric(path, h_unit, v_unit, canonical)
            errors.append(
                f"Metric units declared (horizontal={h_unit or '?'}, "
                f"vertical={v_unit or '?'}); TOTaLi requires {canonical}. "
                "Rerun ingestion after unit conversion; no silent reprojection."
            )
            return errors

        if canonical in self._US_SURVEY_FOOT_ALIASES:
            allowed_aliases = self._US_SURVEY_FOOT_ALIASES
        else:
            allowed_aliases = frozenset({canonical})

        def _emit_mismatch(axis: str, declared: str) -> None:
            self.audit.log(
                "unit_rejected",
                {
                    "file": str(path),
                    "axis": axis,
                    "declared_unit": declared,
                    "horizontal_unit": h_unit,
                    "vertical_unit": v_unit,
                    "expected_unit": canonical,
                    "tolerance_ft": self.unit_tolerance_ft,
                    "reason": "unit_mismatch",
                },
            )

        if h_unit and h_unit not in allowed_aliases:
            _emit_mismatch("horizontal", h_unit)
            errors.append(
                f"Horizontal unit {h_unit!r} is not compatible with canonical "
                f"{canonical!r} (tolerance_ft={self.unit_tolerance_ft})"
            )
        if v_unit and v_unit not in allowed_aliases:
            _emit_mismatch("vertical", v_unit)
            errors.append(
                f"Vertical unit {v_unit!r} is not compatible with canonical "
                f"{canonical!r} (tolerance_ft={self.unit_tolerance_ft})"
            )

        if not errors:
            self.audit.log(
                "unit_validated",
                {
                    "file": str(path),
                    "horizontal_unit": h_unit,
                    "vertical_unit": v_unit,
                    "canonical_unit": canonical,
                    "tolerance_ft": self.unit_tolerance_ft,
                },
            )

        return errors

    def _reject_metric(self, path: Path, h_unit: str, v_unit: str, canonical: str) -> None:
        """G-3: canonical metric-rejection audit event."""
        self.audit.log(
            "unit_rejected",
            {
                "file": str(path),
                "horizontal_unit": h_unit,
                "vertical_unit": v_unit,
                "expected_unit": canonical,
                "tolerance_ft": self.unit_tolerance_ft,
                "reason": "metric_not_allowed",
            },
        )

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
