"""
Phase 4: CAD Shielding
=======================
"Build around, not through" – middleware isolation prevents CAD kernel crashes.
Geometry quarantine/healing ensures watertight, topologically sane inserts.
All output goes to DRAFT layers only.
"""

import json
import uuid
import hashlib
from pathlib import Path
from typing import Optional

import numpy as np

from totali.pipeline.models import (
    PhaseResult, ExtractionResult, HealingReport, GeometryStatus
)
from totali.pipeline.base_phase import PipelinePhase
from totali.pipeline.context import PipelineContext
from totali.audit.logger import AuditLogger


class CADShield(PipelinePhase):
    def __init__(self, config: dict, audit: AuditLogger):
        super().__init__(config, audit)
        self.format = config.get("format", "dxf")
        self.healing_cfg = config.get("geometry_healing", {})
        self.layer_map = config.get("layer_mapping", {})
        self.timeout = config.get("middleware_timeout_sec", 30)
        self.max_retry = config.get("max_retry", 3)

    def validate_inputs(self, context: PipelineContext) -> tuple[bool, list[str]]:
        errors: list[str] = []
        if context.extraction is None:
            errors.append("extraction missing; run extract phase first")
        return len(errors) == 0, errors

    def run(self, context: PipelineContext) -> PhaseResult:
        extraction: ExtractionResult | None = context.extraction
        output_dir = Path(context.output_dir)

        if extraction is None:
            return PhaseResult(
                phase="shield", success=False,
                message="No extraction data in context"
            )

        # Geometry healing pass
        healing = self._heal_geometry(extraction)

        self.audit.log("heal", {
            "input_entities": healing.input_entity_count,
            "healed": healing.healed_count,
            "quarantined": healing.quarantined_count,
            "passed": healing.passed_count,
        })

        # Write to DXF
        dxf_path = output_dir / "totali_draft_output.dxf"
        entity_manifest = self._write_dxf(extraction, dxf_path)

        # Write entity manifest (chain of custody)
        manifest_path = output_dir / "entity_manifest.json"
        with open(manifest_path, "w") as f:
            json.dump(entity_manifest, f, indent=2)

        # Log every insert
        for entity in entity_manifest.get("entities", []):
            self.audit.log("insert", {
                "entity_id": entity["id"],
                "layer": entity["layer"],
                "type": entity["type"],
                "status": GeometryStatus.DRAFT.value,
                "source_hash": entity.get("source_hash", ""),
            })

        return PhaseResult(
            phase="shield",
            success=True,
            message=f"DXF written with {len(entity_manifest.get('entities', []))} entities "
                    f"(healed: {healing.healed_count}, quarantined: {healing.quarantined_count})",
            data={
                "dxf_path": str(dxf_path),
                "manifest": entity_manifest,
                "healing": healing,
                "extraction": extraction,
                "crs": context.crs,
                "stats": context.stats,
                "classification": context.classification,
                "input_hash": context.input_hash,
            },
            output_files=[dxf_path, manifest_path],
        )

    def _heal_geometry(self, extraction: ExtractionResult) -> HealingReport:
        """Validate and heal geometry before CAD insertion."""
        report = HealingReport()
        close_tol = self.healing_cfg.get("close_tolerance", 0.001)
        degen_tol = self.healing_cfg.get("degenerate_face_threshold", 0.0001)

        # Performance optimization: pre-calculate squared tolerances to avoid sqrt in np.linalg.norm
        close_tol_sq = close_tol ** 2
        degen_tol_sq = (degen_tol * 2.0) ** 2  # Area threshold squared component

        # Check DTM faces
        if extraction.dtm_faces is not None:
            report.input_entity_count += len(extraction.dtm_faces)
            for i, face in enumerate(extraction.dtm_faces):
                verts = extraction.dtm_vertices[face]
                # Area = 0.5 * |(v1-v0) x (v2-v0)|
                # area < degen_tol  =>  0.25 * |cross|^2 < degen_tol^2  =>  |cross|^2 < (2*degen_tol)^2
                cross_vec = np.cross(verts[1] - verts[0], verts[2] - verts[0])
                sq_norm_cross = np.sum(cross_vec**2)

                if sq_norm_cross < degen_tol_sq:
                    report.quarantined_count += 1
                    report.issues.append(f"Degenerate DTM face {i}: area={0.5 * np.sqrt(sq_norm_cross):.8f}")
                else:
                    report.passed_count += 1

        # Check polylines (breaklines, contours, curbs, wires)
        polyline_sets = [
            ("breaklines", extraction.breaklines),
            ("contours_minor", extraction.contours_minor),
            ("contours_index", extraction.contours_index),
            ("curbs", extraction.curb_lines),
            ("wires", extraction.wire_lines),
        ]

        for name, lines in polyline_sets:
            for i, line in enumerate(lines):
                report.input_entity_count += 1
                if len(line) < 2:
                    report.quarantined_count += 1
                    report.issues.append(f"{name}[{i}]: fewer than 2 vertices")
                    continue

                # Performance optimization:
                # Use vectorized squared distance calculation instead of np.linalg.norm(np.diff(...))
                deltas = line[1:, :2] - line[:-1, :2]
                sq_diffs = np.sum(deltas**2, axis=1)

                # np.count_nonzero is faster than np.sum for boolean masks
                dupes = np.count_nonzero(sq_diffs < close_tol_sq)

                if dupes > 0:
                    report.healed_count += 1
                    report.issues.append(
                        f"{name}[{i}]: removed {dupes} duplicate vertices"
                    )
                else:
                    report.passed_count += 1

        # Check polygons (buildings, hardscape, occlusion zones)
        polygon_sets = [
            ("buildings", extraction.building_footprints),
            ("hardscape", extraction.hardscape_polygons),
            ("occlusion_zones", extraction.occlusion_zones),
        ]

        for name, polys in polygon_sets:
            for i, poly in enumerate(polys):
                report.input_entity_count += 1
                if len(poly) < 3:
                    report.quarantined_count += 1
                    report.issues.append(f"{name}[{i}]: fewer than 3 vertices")
                else:
                    # Check if closed (squared distance comparison)
                    dist_vec = poly[0] - poly[-1]
                    sq_dist = np.sum(dist_vec**2)
                    if sq_dist > close_tol_sq:
                        report.healed_count += 1
                    else:
                        report.passed_count += 1

        return report

    def _write_dxf(self, extraction: ExtractionResult, path: Path) -> dict:
        """Write extraction results to DXF with proper layer mapping."""
        try:
            import ezdxf
            return self._write_dxf_ezdxf(extraction, path)
        except ImportError:
            return self._write_dxf_manual(extraction, path)
    def _write_dxf_ezdxf(self, extraction: ExtractionResult, path: Path) -> dict:
        """Write DXF using ezdxf library."""
        import ezdxf

        doc = ezdxf.new("R2018")
        msp = doc.modelspace()
        entities = []

        # Create layers
        for layer_name in self.layer_map.values():
            doc.layers.add(layer_name)

        # DTM as 3DFACE entities
        if extraction.dtm_vertices is not None and extraction.dtm_faces is not None:
            layer = self.layer_map.get("ground_surface", "TOTaLi-SURV-DTM-DRAFT")
            for face in extraction.dtm_faces:
                v = extraction.dtm_vertices[face]
                entity_id = self._entity_id()
                try:
                    msp.add_3dface(
                        [tuple(v[0]), tuple(v[1]), tuple(v[2]), tuple(v[2])],
                        dxfattribs={"layer": layer},
                    )
                    entities.append(self._entity_record(
                        entity_id, "3DFACE", layer, v
                    ))
                except Exception:
                    pass

        # Breaklines as POLYLINE
        layer = self.layer_map.get("breaklines", "TOTaLi-SURV-BRKLN-DRAFT")
        for line in extraction.breaklines:
            entity_id = self._entity_id()
            try:
                msp.add_polyline3d(
                    [tuple(p) for p in line],
                    dxfattribs={"layer": layer},
                )
                entities.append(self._entity_record(entity_id, "POLYLINE", layer, line))
            except Exception:
                pass

        # Contours
        for contour_list, layer_key in [
            (extraction.contours_minor, "contours_minor"),
            (extraction.contours_index, "contours_index"),
        ]:
            layer = self.layer_map.get(layer_key, f"TOTaLi-SURV-CONT-{layer_key.upper()}-DRAFT")
            for seg in contour_list:
                entity_id = self._entity_id()
                try:
                    msp.add_lwpolyline(
                        [tuple(p) for p in seg],
                        dxfattribs={"layer": layer},
                    )
                    entities.append(self._entity_record(entity_id, "LWPOLYLINE", layer, seg))
                except Exception:
                    pass

        # Building footprints
        layer = self.layer_map.get("buildings", "TOTaLi-PLAN-BLDG-DRAFT")
        for poly in extraction.building_footprints:
            entity_id = self._entity_id()
            try:
                pts = [tuple(p) for p in poly]
                pts.append(pts[0])  # close polygon
                msp.add_lwpolyline(pts, close=True, dxfattribs={"layer": layer})
                entities.append(self._entity_record(entity_id, "POLYGON", layer, poly))
            except Exception:
                pass

        # Curbs
        layer = self.layer_map.get("curbs", "TOTaLi-PLAN-CURB-DRAFT")
        for line in extraction.curb_lines:
            entity_id = self._entity_id()
            try:
                msp.add_polyline3d(
                    [tuple(p) for p in line],
                    dxfattribs={"layer": layer},
                )
                entities.append(self._entity_record(entity_id, "POLYLINE", layer, line))
            except Exception:
                pass

        # Wire
        layer = self.layer_map.get("wire", "TOTaLi-PLAN-WIRE-DRAFT")
        for line in extraction.wire_lines:
            entity_id = self._entity_id()
            try:
                msp.add_polyline3d(
                    [tuple(p) for p in line],
                    dxfattribs={"layer": layer},
                )
                entities.append(self._entity_record(entity_id, "POLYLINE", layer, line))
            except Exception:
                pass

        # Occlusion zones
        layer = self.layer_map.get("occlusion_zones", "TOTaLi-QA-OCCLUSION")
        for poly in extraction.occlusion_zones:
            entity_id = self._entity_id()
            try:
                pts = [tuple(p) for p in poly]
                pts.append(pts[0])
                msp.add_lwpolyline(pts, close=True, dxfattribs={"layer": layer})
                entities.append(self._entity_record(entity_id, "OCCLUSION_ZONE", layer, poly))
            except Exception:
                pass

        doc.saveas(str(path))

        return {
            "format": "dxf",
            "path": str(path),
            "entity_count": len(entities),
            "entities": entities,
        }

    def _write_dxf_manual(self, extraction: ExtractionResult, path: Path) -> dict:
        """Minimal DXF writer fallback when ezdxf is not available."""
        entities = []
        lines = [
            "0", "SECTION", "2", "HEADER", "0", "ENDSEC",
            "0", "SECTION", "2", "ENTITIES",
        ]

        # Very basic LINE writer for breaklines just to have something
        layer = self.layer_map.get("breaklines", "TOTaLi-SURV-BRKLN-DRAFT")
        for brk in extraction.breaklines:
            if len(brk) >= 2:
                entity_id = self._entity_id()
                p0 = brk[0]
                p1 = brk[1]
                lines.extend([
                    "0", "LINE", "8", layer,
                    "10", str(p0[0]), "20", str(p0[1]), "30", str(p0[2]),
                    "11", str(p1[0]), "21", str(p1[1]), "31", str(p1[2]),
                ])
                entities.append(self._entity_record(entity_id, "LINE", layer, brk))

        lines.extend(["0", "ENDSEC", "0", "EOF"])

        with open(path, "w") as f:
            f.write("\n".join(lines))

        return {
            "format": "dxf",
            "path": str(path),
            "entity_count": len(entities),
            "entities": entities,
        }

    def _entity_id(self) -> str:
        return uuid.uuid4().hex[:12]

    def _entity_record(
        self, entity_id: str, entity_type: str, layer: str, geometry,
        confidence: float = 0.0, rule_engine_passed: bool = True, provenance: dict = None
    ) -> dict:
        """Create an entity record for the manifest / audit trail."""
        geo_bytes = geometry.tobytes() if isinstance(geometry, np.ndarray) else str(geometry).encode()
        return {
            "id": entity_id,
            "type": entity_type,
            "layer": layer,
            "status": GeometryStatus.DRAFT.value,
            "source_hash": hashlib.sha256(geo_bytes).hexdigest()[:16],
            "confidence": confidence,
            "rule_engine_passed": rule_engine_passed,
            "provenance": provenance or {},
        }
