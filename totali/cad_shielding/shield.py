"""
Phase 4: CAD Shielding
=======================
"Build around, not through" – middleware isolation prevents CAD kernel crashes.
Geometry quarantine/healing ensures watertight, topologically sane inserts.
All output goes to DRAFT layers only.
"""

import json
import re
import uuid
import itertools
import hashlib
from pathlib import Path

import numpy as np

from totali.pipeline.models import (
    PhaseResult, ExtractionResult, HealingReport, GeometryStatus
)
from totali.pipeline.base_phase import PipelinePhase
from totali.pipeline.context import PipelineContext
from totali.audit.logger import AuditLogger
from totali.cad_shielding.geometry_healer import GeometryHealer, HealingConfig


# C-3: TOTaLi invariant §1.3 — every emitted layer must end in -DRAFT
# (TOTaLi-<DISC>-<FEAT>-DRAFT), with TOTaLi-QA-* exempt. Enforced at
# config load so a typo in pipeline.yaml fails the phase before any DXF.
_LAYER_NAME_RE = re.compile(
    r"^TOTaLi-[A-Z0-9]+(?:-[A-Z0-9_]+)+-DRAFT$|^TOTaLi-QA-[A-Z0-9_-]+$"
)


class NonConformingLayerName(ValueError):
    """C-3: layer_mapping contains a non-conforming layer name."""


class UnsupportedCADFormat(ValueError):
    """C-4: cad_shielding.format is not yet implemented (dwg stub / dgn deferred)."""


# C-4: format → status. Single source of truth; flip "dwg" to "supported"
# in one place when the dwg-tool-parser writer lands.
_FORMAT_STATUS: dict[str, str] = {
    "dxf": "supported",
    "dwg": "stub",
    "dgn": "deferred",
}


class CADShield(PipelinePhase):
    def __init__(self, config: dict, audit: AuditLogger):
        super().__init__(config, audit)
        self.format = str(config.get("format", "dxf")).lower()
        self.healing_cfg = config.get("geometry_healing", {})
        self.layer_map = config.get("layer_mapping", {})
        self.timeout = config.get("middleware_timeout_sec", 30)
        self.max_retry = config.get("max_retry", 3)

        # C-4 then C-3: fail fast on misconfigured deployment.
        self._validate_format(self.format)
        self._validate_layer_mapping(self.layer_map)

        # C-2: real geometry healing. Repairs what it can and quarantines
        # (excludes) what it can't, so degenerate geometry never reaches the DXF.
        self.healer = GeometryHealer(
            HealingConfig(
                close_tolerance=self.healing_cfg.get("close_tolerance", 0.001),
                degenerate_threshold=self.healing_cfg.get("degenerate_face_threshold", 0.0001),
                snap_tolerance=self.healing_cfg.get("snap_tolerance", 0.0),
                check_self_intersection=self.healing_cfg.get("self_intersection_check", True),
                repair_self_intersection=self.healing_cfg.get("repair_self_intersection", True),
                weld_vertices=self.healing_cfg.get("weld_vertices", True),
                remove_duplicates=self.healing_cfg.get("remove_duplicates", True),
                close_polygons=self.healing_cfg.get("close_polygons", True),
            )
        )

        self._id_prefix = uuid.uuid4().hex[:6]
        self._id_counter = itertools.count()

    @staticmethod
    def _validate_format(fmt: str) -> None:
        """C-4: reject non-DXF formats at construction."""
        status = _FORMAT_STATUS.get(fmt)
        if status is None:
            raise ValueError(
                f"cad_shielding.format={fmt!r} is not recognised. "
                f"Allowed: {sorted(_FORMAT_STATUS)}"
            )
        if status == "supported":
            return
        raise UnsupportedCADFormat(
            f"cad_shielding.format={fmt!r} is {status!r}. "
            f"Only 'dxf' is currently implemented."
        )

    @staticmethod
    def _validate_layer_mapping(mapping: dict) -> None:
        """C-3: reject non-conforming layer names at config load."""
        bad = [v for v in mapping.values() if not _LAYER_NAME_RE.match(v)]
        if bad:
            raise NonConformingLayerName(
                f"layer_mapping contains non-conforming names: {bad}. "
                f"Required: TOTaLi-<DISC>-<FEAT>-DRAFT (or TOTaLi-QA-*). "
                f"TOTaLi §1.3 invariant — fix the config, do not exempt."
            )

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
        entity_manifest = self._write_dxf(extraction, dxf_path, context)

        # U4: the DXF deliverable must carry a reference back to the audit chain
        # (raw input hash + audit log) so the stampable output is verifiable
        # end-to-end against its chain of custody.
        entity_manifest["audit_reference"] = {
            "input_hash": context.input_hash,
            "audit_log": str(self.audit.log_path),
        }

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
        """Heal geometry before CAD insertion.

        C-2: healable geometry is repaired in place; geometry that cannot be
        healed is EXCLUDED (quarantined) so it never reaches the DXF. The branch
        previously only *counted* issues and wrote degenerate geometry anyway.
        """
        report = HealingReport()
        degen_tol = self.healing_cfg.get("degenerate_face_threshold", 0.0001)

        # DTM mesh: heal, then drop degenerate faces from the written set.
        if extraction.dtm_vertices is not None and extraction.dtm_faces is not None:
            report.input_entity_count += len(extraction.dtm_faces)
            healed_v, healed_f, mesh_issues = self.healer.heal_mesh(
                extraction.dtm_vertices, extraction.dtm_faces, "dtm_mesh"
            )
            report.issues.extend(mesh_issues)
            if healed_v is None or healed_f is None or len(healed_f) == 0:
                report.quarantined_count += len(extraction.dtm_faces)
                extraction.dtm_faces = np.empty((0, 3), dtype=int)
            else:
                kept = []
                for i, face in enumerate(healed_f):
                    verts = healed_v[face]
                    area = 0.5 * np.linalg.norm(
                        np.cross(verts[1] - verts[0], verts[2] - verts[0])
                    )
                    if area < degen_tol:
                        report.quarantined_count += 1
                        report.issues.append(f"Degenerate DTM face {i}: area={area:.8f}")
                    else:
                        kept.append(face)
                        report.passed_count += 1
                extraction.dtm_vertices = healed_v
                extraction.dtm_faces = np.array(kept) if kept else np.empty((0, 3), dtype=int)

        for name, lines in [
            ("breaklines", extraction.breaklines),
            ("contours_minor", extraction.contours_minor),
            ("contours_index", extraction.contours_index),
            ("curbs", extraction.curb_lines),
            ("wires", extraction.wire_lines),
        ]:
            self._heal_entities(lines, name, self.healer.heal_polyline, report)

        for name, polys in [
            ("buildings", extraction.building_footprints),
            ("hardscape", extraction.hardscape_polygons),
            ("occlusion_zones", extraction.occlusion_zones),
        ]:
            self._heal_entities(polys, name, self.healer.heal_polygon, report)

        return report

    def _heal_entities(self, items: list, name: str, heal_fn, report: HealingReport) -> None:
        """Heal each entity IN PLACE; quarantined (None) entities are dropped from
        the list so they are excluded from the DXF deliverable (C-2)."""
        kept = []
        for i, geom in enumerate(items):
            report.input_entity_count += 1
            healed, issues = heal_fn(geom, f"{name}[{i}]")
            report.issues.extend(issues)
            if healed is None:
                report.quarantined_count += 1
                continue
            kept.append(healed)
            if issues:
                report.healed_count += 1
            else:
                report.passed_count += 1
        items[:] = kept

    def _write_dxf(self, extraction: ExtractionResult, path: Path, context: PipelineContext = None) -> dict:
        """Write extraction results to DXF with proper layer mapping."""
        try:
            import ezdxf  # noqa: F401 — availability probe
            return self._write_dxf_ezdxf(extraction, path, context)
        except ImportError:
            return self._write_dxf_manual(extraction, path, context)

    def _write_dxf_ezdxf(self, extraction: ExtractionResult, path: Path, context: PipelineContext = None) -> dict:
        """Write DXF using ezdxf library."""
        import ezdxf

        doc = ezdxf.new("R2018")
        msp = doc.modelspace()
        entities = []

        # Create layers
        for layer_name in self.layer_map.values():
            doc.layers.add(self._sanitize_dxf_string(layer_name))

        # Coded survey shots (authoritative points on conforming DRAFT layers)
        for pt in extraction.coded_survey_points:
            entity_id = self._entity_id()
            try:
                if pt.draft_layer not in doc.layers:
                    doc.layers.add(pt.draft_layer)
                msp.add_point(
                    (pt.x, pt.y, pt.z),
                    dxfattribs={"layer": pt.draft_layer},
                )
                entities.append(self._entity_record(
                    entity_id, "POINT", pt.draft_layer,
                    np.array([pt.x, pt.y, pt.z]),
                    confidence=1.0,
                    provenance={
                        "point_id": pt.point_id,
                        "field_code": pt.field_code,
                        "firm_layer": pt.firm_layer,
                        "authoritative": True,
                    },
                ))
            except Exception:
                pass

        # DTM as 3DFACE entities
        if extraction.dtm_vertices is not None and extraction.dtm_faces is not None:
            layer = self._sanitize_dxf_string(self.layer_map.get("ground_surface", "TOTaLi-SURV-DTM-DRAFT"))
            for face in extraction.dtm_faces:
                v = extraction.dtm_vertices[face]
                entity_id = self._entity_id()
                try:
                    msp.add_3dface(
                        [tuple(v[0]), tuple(v[1]), tuple(v[2]), tuple(v[2])],
                        dxfattribs={"layer": self._sanitize_dxf_string(layer)},
                    )
                    entities.append(self._entity_record(
                        entity_id, "3DFACE", layer, v
                    ))
                except Exception:
                    pass

        # Breaklines as POLYLINE
        layer = self._sanitize_dxf_string(self.layer_map.get("breaklines", "TOTaLi-SURV-BRKLN-DRAFT"))
        for line in extraction.breaklines:
            entity_id = self._entity_id()
            try:
                msp.add_polyline3d(
                    [tuple(p) for p in line],
                    dxfattribs={"layer": self._sanitize_dxf_string(layer)},
                )
                entities.append(self._entity_record(entity_id, "POLYLINE", layer, line))
            except Exception:
                pass

        # Contours
        for contour_list, layer_key in [
            (extraction.contours_minor, "contours_minor"),
            (extraction.contours_index, "contours_index"),
        ]:
            layer = self._sanitize_dxf_string(self.layer_map.get(layer_key, f"TOTaLi-SURV-CONT-{layer_key.upper()}-DRAFT"))
            for seg in contour_list:
                entity_id = self._entity_id()
                try:
                    msp.add_lwpolyline(
                        [tuple(p) for p in seg],
                        dxfattribs={"layer": self._sanitize_dxf_string(layer)},
                    )
                    entities.append(self._entity_record(entity_id, "LWPOLYLINE", layer, seg))
                except Exception:
                    pass

        # Building footprints
        layer = self._sanitize_dxf_string(self.layer_map.get("buildings", "TOTaLi-PLAN-BLDG-DRAFT"))
        for poly in extraction.building_footprints:
            entity_id = self._entity_id()
            try:
                pts = [tuple(p) for p in poly]
                pts.append(pts[0])  # close polygon
                msp.add_lwpolyline(pts, close=True, dxfattribs={"layer": self._sanitize_dxf_string(layer)})
                entities.append(self._entity_record(entity_id, "POLYGON", layer, poly))
            except Exception:
                pass

        # Curbs
        layer = self._sanitize_dxf_string(self.layer_map.get("curbs", "TOTaLi-PLAN-CURB-DRAFT"))
        for line in extraction.curb_lines:
            entity_id = self._entity_id()
            try:
                msp.add_polyline3d(
                    [tuple(p) for p in line],
                    dxfattribs={"layer": self._sanitize_dxf_string(layer)},
                )
                entities.append(self._entity_record(entity_id, "POLYLINE", layer, line))
            except Exception:
                pass

        # Wire
        layer = self._sanitize_dxf_string(self.layer_map.get("wire", "TOTaLi-PLAN-WIRE-DRAFT"))
        for line in extraction.wire_lines:
            entity_id = self._entity_id()
            try:
                msp.add_polyline3d(
                    [tuple(p) for p in line],
                    dxfattribs={"layer": self._sanitize_dxf_string(layer)},
                )
                entities.append(self._entity_record(entity_id, "POLYLINE", layer, line))
            except Exception:
                pass

        # Occlusion zones
        layer = self._sanitize_dxf_string(self.layer_map.get("occlusion_zones", "TOTaLi-QA-OCCLUSION"))
        for poly in extraction.occlusion_zones:
            entity_id = self._entity_id()
            try:
                pts = [tuple(p) for p in poly]
                pts.append(pts[0])
                msp.add_lwpolyline(pts, close=True, dxfattribs={"layer": self._sanitize_dxf_string(layer)})
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

    def _write_dxf_manual(self, extraction: ExtractionResult, path: Path, context: PipelineContext = None) -> dict:
        """Minimal DXF writer fallback when ezdxf is not available."""
        entities = []
        lines = [
            "0", "SECTION", "2", "HEADER", "0", "ENDSEC",
            "0", "SECTION", "2", "ENTITIES",
        ]

        # Write breaklines as LINE entities
        layer = self._sanitize_dxf_string(self.layer_map.get("breaklines", "TOTaLi-SURV-BRKLN-DRAFT"))
        for brk in extraction.breaklines:
            for i in range(len(brk) - 1):
                entity_id = self._entity_id()
                p0, p1 = brk[i], brk[i + 1]
                lines.extend([
                    "0", "LINE",
                    "8", self._sanitize_dxf_string(layer),
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
        return f"{self._id_prefix}{next(self._id_counter):06x}"

    def _entity_record(
        self, entity_id: str, entity_type: str, layer: str, geometry,
        confidence: float = 1.0, rule_engine_passed: bool = True,
        provenance: dict = None
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

    def _sanitize_dxf_string(self, text: str) -> str:
        """Sanitize strings to prevent DXF injection by removing newlines."""
        if not isinstance(text, str):
            return str(text)
        return text.replace("\n", " ").replace("\r", " ")
