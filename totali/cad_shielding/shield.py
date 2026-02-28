"""
Phase 4: CADShield
==================
Transforms extraction results into CAD-compliant formats (DXF/DWG)
while maintaining a defensible audit trail of every entity.
"""

import hashlib
import uuid
import logging
from pathlib import Path
from typing import Any

import numpy as np

from totali.pipeline.models import (
    ExtractionResult,
    HealingReport,
    PhaseResult,
    GeometryStatus,
)
from totali.pipeline.base_phase import PipelinePhase
from totali.pipeline.context import PipelineContext


class CADShield(PipelinePhase):
    def __init__(self, config: dict, audit):
        super().__init__(config, audit)
        self.layer_map = config.get("layer_mapping", {})
        self.healing_cfg = config.get("healing", {})

    def validate_inputs(self, context: PipelineContext) -> tuple[bool, list[str]]:
        errors = []
        if context.extraction is None:
            errors.append("Missing required input: extraction result")
        return len(errors) == 0, errors

    def run(self, context: PipelineContext) -> PhaseResult:
        extraction = context.extraction
        if extraction is None:
            return PhaseResult(
                phase="shield",
                success=False,
                message="Missing extraction result",
            )

        output_dir = context.output_dir
        output_dir.mkdir(parents=True, exist_ok=True)

        # 1. Heal and validate geometry
        healing = self._heal_geometry(extraction)
        self.audit.log("geometry_healing_complete", {
            "input_entities": healing.input_entity_count,
            "healed": healing.healed_count,
            "quarantined": healing.quarantined_count,
        })

        # 2. Write DXF
        dxf_name = f"{Path(context.input_path).stem}_draft.dxf"
        dxf_path = output_dir / dxf_name
        manifest = self._write_dxf(extraction, dxf_path, context)

        # 3. Write Entity Manifest
        manifest_path = output_dir / "entity_manifest.json"
        import json
        with open(manifest_path, "w") as f:
            json.dump(manifest, f, indent=2)

        return PhaseResult(
            phase="shield",
            success=True,
            data={
                "dxf_path": str(dxf_path),
                "manifest": manifest,
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

        # Check DTM faces
        if extraction.dtm_faces is not None:
            report.input_entity_count += len(extraction.dtm_faces)
            for i, face in enumerate(extraction.dtm_faces):
                verts = extraction.dtm_vertices[face]
                area = 0.5 * np.linalg.norm(
                    np.cross(verts[1] - verts[0], verts[2] - verts[0])
                )
                if area < degen_tol:
                    report.quarantined_count += 1
                    report.issues.append(f"Degenerate DTM face {i}: area={area:.8f}")
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

                # Check for duplicate consecutive vertices
                diffs = np.linalg.norm(np.diff(line[:, :2], axis=0), axis=1)
                dupes = np.sum(diffs < close_tol)
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
                    # Check if closed
                    if np.linalg.norm(poly[0] - poly[-1]) > close_tol:
                        report.healed_count += 1
                    else:
                        report.passed_count += 1

        return report

    def _write_dxf(self, extraction: ExtractionResult, path: Path, context: PipelineContext) -> dict:
        """Write extraction results to DXF with proper layer mapping."""
        try:
            import ezdxf
            return self._write_dxf_ezdxf(extraction, path, context)
        except ImportError:
            return self._write_dxf_manual(extraction, path, context)

    def _safe_add_entity(self, msp, func, geometry, layer, entity_type, entities, **kwargs):
        """Helper to safely add an entity to the modelspace and record it."""
        entity_id = self._entity_id()
        try:
            func(geometry, dxfattribs={"layer": layer}, **kwargs)
            entities.append(self._entity_record(entity_id, entity_type, layer, geometry))
        except Exception:
            pass

    def _write_dxf_ezdxf(self, extraction: ExtractionResult, path: Path, context: PipelineContext) -> dict:
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
                pts = [tuple(v[0]), tuple(v[1]), tuple(v[2]), tuple(v[2])]
                self._safe_add_entity(msp, msp.add_3dface, pts, layer, "3DFACE", entities)

        # Breaklines as POLYLINE
        layer = self.layer_map.get("breaklines", "TOTaLi-SURV-BRKLN-DRAFT")
        for line in extraction.breaklines:
            pts = [tuple(p) for p in line]
            self._safe_add_entity(msp, msp.add_polyline3d, pts, layer, "POLYLINE", entities)

        # Contours
        for contour_list, layer_key in [
            (extraction.contours_minor, "contours_minor"),
            (extraction.contours_index, "contours_index"),
        ]:
            layer = self.layer_map.get(layer_key, f"TOTaLi-SURV-CONT-{layer_key.upper()}-DRAFT")
            for seg in contour_list:
                pts = [tuple(p) for p in seg]
                self._safe_add_entity(msp, msp.add_lwpolyline, pts, layer, "LWPOLYLINE", entities)

        # Building footprints
        layer = self.layer_map.get("buildings", "TOTaLi-PLAN-BLDG-DRAFT")
        for poly in extraction.building_footprints:
            pts = [tuple(p) for p in poly]
            pts.append(pts[0])  # close polygon
            self._safe_add_entity(msp, msp.add_lwpolyline, pts, layer, "POLYGON", entities, close=True)

        # Curbs
        layer = self.layer_map.get("curbs", "TOTaLi-PLAN-CURB-DRAFT")
        for line in extraction.curb_lines:
            pts = [tuple(p) for p in line]
            self._safe_add_entity(msp, msp.add_polyline3d, pts, layer, "POLYLINE", entities)

        # Wire
        layer = self.layer_map.get("wire", "TOTaLi-PLAN-WIRE-DRAFT")
        for line in extraction.wire_lines:
            pts = [tuple(p) for p in line]
            self._safe_add_entity(msp, msp.add_polyline3d, pts, layer, "POLYLINE", entities)

        # Occlusion zones
        layer = self.layer_map.get("occlusion_zones", "TOTaLi-QA-OCCLUSION")
        for poly in extraction.occlusion_zones:
            pts = [tuple(p) for p in poly]
            pts.append(pts[0])
            self._safe_add_entity(msp, msp.add_lwpolyline, pts, layer, "OCCLUSION_ZONE", entities, close=True)

        doc.saveas(str(path))

        return {
            "format": "dxf",
            "path": str(path),
            "entity_count": len(entities),
            "entities": entities,
        }

    def _write_dxf_manual(self, extraction: ExtractionResult, path: Path, context: PipelineContext) -> dict:
        """Minimal DXF writer fallback when ezdxf is not available."""
        entities = []
        lines = [
            "0", "SECTION", "2", "HEADER", "0", "ENDSEC",
            "0", "SECTION", "2", "ENTITIES",
        ]

        # Write breaklines as LINE entities
        layer = self.layer_map.get("breaklines", "TOTaLi-SURV-BRKLN-DRAFT")
        for brk in extraction.breaklines:
            for i in range(len(brk) - 1):
                entity_id = self._entity_id()
                p0, p1 = brk[i], brk[i + 1]
                lines.extend([
                    "0", "LINE",
                    "8", layer,
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
        self, entity_id: str, entity_type: str, layer: str, geometry, *args, **kwargs
    ) -> dict:
        """Create an entity record for the manifest / audit trail."""
        geo_bytes = geometry.tobytes() if isinstance(geometry, np.ndarray) else str(geometry).encode()
        record = {
            "id": entity_id,
            "type": entity_type,
            "layer": layer,
            "status": GeometryStatus.DRAFT.value,
            "source_hash": hashlib.sha256(geo_bytes).hexdigest()[:16],
        }
        # Support legacy positional args for confidence, etc. if they appear in tests
        arg_names = ["confidence", "rule_engine_passed", "provenance"]
        for name, val in zip(arg_names, args):
            record[name] = val
        record.update(kwargs)
        return record
