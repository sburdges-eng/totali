"""
Phase 1: Geodetic Gatekeeper
=============================
Deterministic ETL – enforces CRS/epoch/unit metadata at ingestion.
Rejects ambiguous inputs. Applies PROJ-based transformations.
"""

import hashlib
import json
import os
from pathlib import Path

import numpy as np
import laspy
import pyproj
from pyproj import CRS, Transformer
from pyproj.exceptions import CRSError

from totali.fieldcodes import DESCRIPTIONS_PATH_ENV, FLD_PATH_ENV, load_field_codes
from totali.pipeline.input_kind import is_coded_survey_input
from totali.segmentation.dataset import parse_asc
from totali.pipeline.models import (
    PhaseResult, CRSMetadata, PointCloudStats
)
from totali.pipeline.base_phase import PipelinePhase
from totali.pipeline.context import PipelineContext
from totali.audit.logger import AuditLogger
from totali.geodetic.crs_inference import (
    CRSInferenceStatus,
    build_jurisdiction,
    infer_crs,
)


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
        # U2: opt-in CRS inference. When enabled, a missing/undeclared CRS is
        # inferred from coordinate bounds against the configured jurisdiction
        # zones; ambiguity routes to quarantine, out-of-jurisdiction rejects.
        # Defaults OFF so legacy ingestion behavior is unchanged.
        self.crs_inference_enabled = config.get("crs_inference_enabled", False)
        self.jurisdiction = build_jurisdiction(config)
        # G-5/G-6: sub-threshold inferred CRS routes to quarantine instead of
        # auto-proceeding. ``auto_assign_high_confidence: false`` sends every
        # inferred CRS to the operator regardless of score.
        self.crs_confidence_threshold = float(config.get("crs_confidence_threshold", 0.8))
        self.auto_assign_high_confidence = config.get("auto_assign_high_confidence", True)
        self.quarantine_ui_port = int(config.get("quarantine_ui_port", 5050))
        self.geoid_model = config.get("geoid_model", "GEOID18")
        allowed_geoids = config.get("allowed_geoid_models")
        if allowed_geoids is None:
            allowed_geoids = [self.geoid_model]
        self.allowed_geoid_models = frozenset(
            self._normalize_geoid(name) for name in allowed_geoids
        )
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
        if is_coded_survey_input(input_path):
            return self._run_coded_survey(context, input_path)
        return self._run_las(context, input_path)

    def _run_las(self, context: PipelineContext, input_path: Path) -> PhaseResult:
        output_dir = Path(context.output_dir)

        # Read point cloud
        las = laspy.read(str(input_path))

        # Extract and validate CRS
        crs_meta = self._extract_crs(las, input_path)

        # Compute stats early — CRS inference (U2) needs coordinate bounds, and
        # stats don't depend on CRS validity.
        stats = self._compute_stats(las, input_path, crs_meta)

        # U2: no declared/resolvable CRS + inference enabled → infer or route.
        if crs_meta.epsg_code == 0 and self.crs_inference_enabled:
            routed = self._resolve_via_inference(
                crs_meta, stats, input_path, output_dir
            )
            if routed is not None:
                return routed

        if not crs_meta.is_valid:
            return PhaseResult(
                phase="geodetic",
                success=False,
                message=f"CRS validation failed: {crs_meta.validation_errors}",
            )

        # Hash input for chain of custody
        input_hash = self._hash_file(input_path)
        self.audit.log("ingest", self._audit_payload({
            "file": str(input_path),
            "sha256": input_hash,
            "point_count": stats.point_count,
            "crs": f"EPSG:{crs_meta.epsg_code}",
            "bounds_min": stats.bounds_min.tolist() if stats.bounds_min is not None else None,
            "bounds_max": stats.bounds_max.tolist() if stats.bounds_max is not None else None,
        }))

        # Transform if needed
        points_xyz, transform_applied = self._apply_transforms(las, crs_meta)

        if transform_applied:
            self.audit.log("transform", self._audit_payload({
                "from_crs": f"EPSG:{crs_meta.epsg_code}",
                "to_crs": f"EPSG:{self.allowed_epsg[0]}",
                "geoid": self.geoid_model,
            }))

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

    def _run_coded_survey(self, context: PipelineContext, input_path: Path) -> PhaseResult:
        """Ingest a Carlson ``.asc``/``.crd`` export (N,E,Z + field code).

        Coded exports carry no LAS CRS metadata; the project ``allowed_crs`` is
        treated as the declared CRS. Bounds are still validated against the
        jurisdiction allowlist when CRS inference is enabled.
        """
        output_dir = Path(context.output_dir)
        fld_path = self.config.get("fieldcode_fld") or os.environ.get(FLD_PATH_ENV)
        if not fld_path:
            return PhaseResult(
                phase="geodetic",
                success=False,
                message=(
                    f"coded survey input requires geodetic.fieldcode_fld or {FLD_PATH_ENV}"
                ),
            )

        desc_path = self.config.get("fieldcode_descriptions") or os.environ.get(
            DESCRIPTIONS_PATH_ENV
        )
        table = load_field_codes(fld_path, desc_path)
        labeled = parse_asc(input_path, table)
        if not labeled:
            return PhaseResult(
                phase="geodetic",
                success=False,
                message="No survey points parsed from coded export",
            )

        if not self.allowed_epsg or self.allowed_epsg[0] is None:
            return PhaseResult(
                phase="geodetic",
                success=False,
                message="allowed_crs must declare the project CRS for coded survey input",
            )

        epsg = self.allowed_epsg[0]
        crs_meta = CRSMetadata(
            epsg_code=epsg,
            is_valid=True,
            geoid_model=self.geoid_model,
            horizontal_unit=self.config.get("elevation_unit", "US_survey_foot"),
            vertical_unit=self.config.get("elevation_unit", "US_survey_foot"),
            epoch=self.config.get("required_epoch"),
            source_datum=next(
                (z.name for z in self.jurisdiction if z.epsg == epsg), None
            ),
        )

        # State Plane convention: X=easting, Y=northing, Z=elevation.
        points_xyz = np.array(
            [[p.easting, p.northing, p.elevation] for p in labeled],
            dtype=np.float64,
        )
        stats = PointCloudStats(
            point_count=len(labeled),
            bounds_min=points_xyz.min(axis=0),
            bounds_max=points_xyz.max(axis=0),
            has_rgb=False,
            has_intensity=False,
            has_classification=False,
            source_file=str(input_path),
            crs=crs_meta,
        )

        if self.crs_inference_enabled:
            routed = self._resolve_via_inference(
                crs_meta, stats, input_path, output_dir
            )
            if routed is not None:
                return routed

        declared_geoid = self._read_declared_geoid(input_path)
        geoid_errors, resolved_geoid = self._validate_geoid(input_path, declared_geoid)
        if geoid_errors:
            return PhaseResult(
                phase="geodetic",
                success=False,
                message=f"Geoid validation failed: {geoid_errors}",
            )
        crs_meta.geoid_model = resolved_geoid

        input_hash = self._hash_file(input_path)
        self.audit.log("ingest", self._audit_payload({
            "file": str(input_path),
            "sha256": input_hash,
            "point_count": stats.point_count,
            "input_kind": "coded_survey",
            "crs": f"EPSG:{crs_meta.epsg_code}",
            "bounds_min": stats.bounds_min.tolist(),
            "bounds_max": stats.bounds_max.tolist(),
        }))

        report_path = output_dir / f"{input_path.stem}_geodetic_report.json"
        report = {
            "input_file": str(input_path),
            "input_hash": input_hash,
            "input_kind": "coded_survey",
            "crs": {
                "epsg": crs_meta.epsg_code,
                "epoch": crs_meta.epoch,
                "geoid": crs_meta.geoid_model,
                "h_unit": crs_meta.horizontal_unit,
                "v_unit": crs_meta.vertical_unit,
                "source": "project_allowed_crs",
            },
            "point_count": stats.point_count,
            "bounds": {
                "min": stats.bounds_min.tolist(),
                "max": stats.bounds_max.tolist(),
            },
            "transform_applied": False,
            "validation_passed": True,
        }
        with open(report_path, "w") as f:
            json.dump(report, f, indent=2)

        return PhaseResult(
            phase="geodetic",
            success=True,
            message="Coded survey ingested with project CRS",
            data={
                "points_xyz": points_xyz,
                "las": None,
                "crs": crs_meta,
                "stats": stats,
                "input_hash": input_hash,
                "input_kind": "coded_survey",
                "coded_points": labeled,
            },
            output_files=[report_path],
        )

    # ------------------------------------------------------------------
    # U2: CRS inference + quarantine routing
    # ------------------------------------------------------------------
    def _resolve_via_inference(
        self,
        crs_meta: CRSMetadata,
        stats: PointCloudStats,
        input_path: Path,
        output_dir: Path,
    ) -> PhaseResult | None:
        """Resolve an undeclared CRS. Returns a terminal PhaseResult to halt
        (quarantine/reject), or ``None`` to proceed (CRS inferred or resolved).
        """
        item_id = input_path.stem
        filename = input_path.name

        # HITL loop closed: honor an operator resolution written back by the
        # quarantine UI on a prior run rather than re-inferring.
        resolution = self._read_operator_resolution(item_id, output_dir)
        if resolution is not None and resolution.get("action") == "confirm":
            epsg = resolution.get("epsg")
            if epsg in self.allowed_epsg:
                self._apply_inferred(
                    crs_meta, epsg, source="operator_resolution",
                    confidence=1.0, requires_review=False, candidates=[],
                    reason="operator confirmed CRS via quarantine UI",
                )
                return None

        result = infer_crs(
            declared_epsg=None,
            bounds_min=stats.bounds_min,
            bounds_max=stats.bounds_max,
            jurisdiction=self.jurisdiction,
            allowed_epsg=self.allowed_epsg,
        )

        if result.status is CRSInferenceStatus.INFERRED:
            cand = result.candidates[0]
            needs_operator = (
                not self.auto_assign_high_confidence
                or cand.confidence < self.crs_confidence_threshold
            )
            if needs_operator:
                self._route_to_quarantine(item_id, filename, stats, output_dir, result)
                return PhaseResult(
                    phase="geodetic",
                    success=False,
                    message=(
                        f"CRS inferred below confidence threshold "
                        f"({cand.confidence:.2f} < {self.crs_confidence_threshold}); "
                        f"routed to quarantine UI :{self.quarantine_ui_port}: "
                        f"{result.reason}"
                    ),
                    data={
                        "quarantined": True,
                        "item_id": item_id,
                        "confidence": cand.confidence,
                        "quarantine_ui_port": self.quarantine_ui_port,
                    },
                )
            self._apply_inferred(
                crs_meta, cand.epsg, source="bounds_inference",
                confidence=cand.confidence, requires_review=True,
                candidates=result.candidates, reason=result.reason,
            )
            return None

        if result.requires_quarantine:
            self._route_to_quarantine(item_id, filename, stats, output_dir, result)
            return PhaseResult(
                phase="geodetic",
                success=False,
                message=f"CRS ambiguous; routed to quarantine: {result.reason}",
                data={"quarantined": True, "item_id": item_id},
            )

        if result.is_rejected:
            self.audit.log("crs_out_of_jurisdiction", self._audit_payload({
                "file": str(input_path),
                "item_id": item_id,
                "reason": result.reason,
                "bounds_min": stats.bounds_min.tolist() if stats.bounds_min is not None else None,
                "bounds_max": stats.bounds_max.tolist() if stats.bounds_max is not None else None,
            }))
            return PhaseResult(
                phase="geodetic",
                success=False,
                message=f"CRS out of jurisdiction; rejected: {result.reason}",
                data={"rejected": True, "item_id": item_id},
            )

        # MISSING with no candidates → defer to the existing reject-missing path.
        return None

    def _apply_inferred(
        self,
        crs_meta: CRSMetadata,
        epsg: int,
        *,
        source: str,
        confidence: float,
        requires_review: bool,
        candidates: list,
        reason: str = "",
    ) -> None:
        """Promote an inferred/resolved EPSG onto the CRS metadata and audit it.

        The inferred CRS is advisory: `requires_review` rides along so downstream
        consumers (and the surveyor) know it was not declared by the source data.
        """
        crs_meta.epsg_code = epsg
        crs_meta.is_valid = True
        crs_meta.validation_errors = []
        crs_meta.source_datum = next(
            (z.name for z in self.jurisdiction if z.epsg == epsg), crs_meta.source_datum
        )
        self.audit.log("crs_inferred", self._audit_payload({
            "epsg": epsg,
            "source": source,
            "confidence": confidence,
            "requires_review": requires_review,
            "candidates": [
                {"epsg": c.epsg, "name": c.name, "confidence": c.confidence}
                for c in candidates
            ],
            "reason": reason,
        }))

    def _read_operator_resolution(self, item_id: str, output_dir: Path) -> dict | None:
        path = Path(output_dir) / f"{item_id}_crs_resolution.json"
        if not path.exists():
            return None
        try:
            return json.loads(path.read_text())
        except (json.JSONDecodeError, OSError):
            return None

    def _route_to_quarantine(
        self,
        item_id: str,
        filename: str,
        stats: PointCloudStats,
        output_dir: Path,
        result,
    ) -> None:
        """Write the authoritative (flask-free) quarantine artifact, best-effort
        enqueue into a live operator UI, and emit the audit event.
        """
        bmin = stats.bounds_min.tolist() if stats.bounds_min is not None else None
        bmax = stats.bounds_max.tolist() if stats.bounds_max is not None else None
        candidates = [
            {"epsg": c.epsg, "name": c.name, "confidence": c.confidence}
            for c in result.candidates
        ]

        artifact = Path(output_dir) / f"{item_id}_crs_quarantine.json"
        artifact.write_text(json.dumps({
            "item_id": item_id,
            "filename": filename,
            "point_count": stats.point_count,
            "bounds_min": bmin,
            "bounds_max": bmax,
            "candidates": candidates,
            "status": result.status.value,
            "reason": result.reason,
        }, indent=2))

        # The JSON artifact above is authoritative and flask-free. The in-memory
        # UI queue is a best-effort side channel — Flask is an optional dep.
        try:
            from totali.quarantine_ui.app import add_to_quarantine
        except Exception:  # pragma: no cover - flask optional
            add_to_quarantine = None
        if add_to_quarantine is not None:
            add_to_quarantine(
                item_id=item_id,
                filename=filename,
                point_count=stats.point_count,
                bounds_min=bmin,
                bounds_max=bmax,
                candidates=candidates,
                output_dir=str(output_dir),
            )

        self.audit.log("crs_quarantined", self._audit_payload({
            "item_id": item_id,
            "filename": filename,
            "candidates": candidates,
            "status": result.status.value,
            "reason": result.reason,
        }))

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

        meta.epoch = self.config.get("required_epoch")

        declared_geoid = self._read_declared_geoid(path)
        geoid_errors, resolved_geoid = self._validate_geoid(path, declared_geoid)
        errors.extend(geoid_errors)
        if resolved_geoid is not None:
            meta.geoid_model = resolved_geoid

        meta.horizontal_unit = self.config.get("elevation_unit", "US_survey_foot")
        meta.vertical_unit = self.config.get("elevation_unit", "US_survey_foot")

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
                self._audit_payload({
                    "file": str(path),
                    "axis": axis,
                    "declared_unit": declared,
                    "horizontal_unit": h_unit,
                    "vertical_unit": v_unit,
                    "expected_unit": canonical,
                    "tolerance_ft": self.unit_tolerance_ft,
                    "reason": "unit_mismatch",
                }),
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
                self._audit_payload({
                    "file": str(path),
                    "horizontal_unit": h_unit,
                    "vertical_unit": v_unit,
                    "canonical_unit": canonical,
                    "tolerance_ft": self.unit_tolerance_ft,
                }),
            )

        return errors

    def _reject_metric(self, path: Path, h_unit: str, v_unit: str, canonical: str) -> None:
        """G-3: canonical metric-rejection audit event."""
        self.audit.log(
            "unit_rejected",
            self._audit_payload({
                "file": str(path),
                "horizontal_unit": h_unit,
                "vertical_unit": v_unit,
                "expected_unit": canonical,
                "tolerance_ft": self.unit_tolerance_ft,
                "reason": "metric_not_allowed",
            }),
        )

    @staticmethod
    def _normalize_geoid(name: str | None) -> str:
        return (name or "").strip().upper().replace(" ", "").replace("-", "")

    def _proj_runtime_context(self) -> dict[str, str]:
        """G-8: reproducibility metadata for every geodetic audit payload."""
        version = getattr(pyproj, "__version__", "unknown")
        proj_version = getattr(pyproj, "proj_version_str", None)
        if proj_version is None:
            proj_version = getattr(pyproj, "PROJ_VERSION_STR", "unknown")
        return {
            "pyproj_version": str(version),
            "proj_version": str(proj_version),
        }

    def _audit_payload(self, data: dict) -> dict:
        return {**data, **self._proj_runtime_context()}

    def _read_declared_geoid(self, path: Path) -> str | None:
        """Optional sidecar ``{stem}_geodetic_meta.json`` with ``geoid_model`` key."""
        meta_path = path.parent / f"{path.stem}_geodetic_meta.json"
        if not meta_path.exists():
            return None
        try:
            payload = json.loads(meta_path.read_text())
        except (json.JSONDecodeError, OSError):
            return None
        declared = payload.get("geoid_model")
        return str(declared) if declared else None

    def _validate_geoid(
        self, path: Path, declared: str | None
    ) -> tuple[list[str], str | None]:
        """G-7: enforce configured GEOID allowlist; never substitute models."""
        errors: list[str] = []
        canonical = self.geoid_model
        canonical_norm = self._normalize_geoid(canonical)

        if canonical_norm not in self.allowed_geoid_models:
            errors.append(
                f"Configured geoid_model {canonical!r} is not on the allowlist: "
                f"{sorted(self.allowed_geoid_models)}"
            )
            return errors, None

        if declared:
            declared_norm = self._normalize_geoid(declared)
            if declared_norm not in self.allowed_geoid_models:
                self._reject_unsupported_geoid(path, declared, canonical)
                errors.append(
                    f"Declared geoid {declared!r} is not allowed; "
                    f"allowlist={sorted(self.allowed_geoid_models)}. "
                    "No silent substitution."
                )
                return errors, None

        self.audit.log(
            "geoid_validated",
            self._audit_payload({
                "file": str(path),
                "declared_geoid": declared,
                "resolved_geoid": canonical,
                "allowlist": sorted(self.allowed_geoid_models),
            }),
        )
        return errors, canonical

    def _reject_unsupported_geoid(
        self, path: Path, declared: str, canonical: str
    ) -> None:
        """G-7: canonical unsupported-geoid audit event."""
        self.audit.log(
            "geoid_rejected",
            self._audit_payload({
                "file": str(path),
                "declared_geoid": declared,
                "expected_geoid": canonical,
                "allowlist": sorted(self.allowed_geoid_models),
                "reason": "geoid_not_allowed",
            }),
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
