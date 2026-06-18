"""Run U4 partner-LAS E2E outside pytest (real laspy/pyproj, no conftest stubs).

Usage:
    export TOTALI_PARTNER_LAS=/path/to/tile.las
    python tools/run_partner_las_e2e.py [--output-dir DIR] [--audit-dir DIR]

Exits 0 on success; prints JSON summary to stdout.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

from totali.audit.logger import AuditLogger
from totali.audit.verify import verify_log
from totali.pipeline.orchestrator import PHASE_ORDER, PipelineOrchestrator


def _gated_config() -> dict:
    """Geodetic gates ACTIVE; CRS inference for USGS LPC tiles without VLR."""
    return {
        "project": {"name": "partner_las_e2e", "version": "0.1.0"},
        "geodetic": {
            "allowed_crs": [
                "EPSG:2231",
                "EPSG:2232",
                "EPSG:2233",
                "EPSG:6428",
                "EPSG:6430",
                "EPSG:26913",
            ],
            "reject_on_missing_crs": True,
            "reject_on_mixed_datum": True,
            "geoid_model": "GEOID18",
            "allowed_geoid_models": ["GEOID18"],
            "elevation_unit": "US_survey_foot",
            "crs_inference_enabled": True,
            "crs_confidence_threshold": 0.8,
            "auto_assign_high_confidence": True,
            "jurisdiction_zones": [
                {
                    "epsg": 26913,
                    "name": "NAD83 / UTM zone 13N (CO San Luis Valley)",
                    "xy_min": [300000, 4100000],
                    "xy_max": [500000, 4400000],
                }
            ],
        },
        "segmentation": {
            "model_path": "models/point_transformer_v2.onnx",
            "device": "cpu",
            "confidence_threshold": 0.75,
            "occlusion_threshold": 0.30,
            "batch_size": 65536,
            "voxel_size": 0.05,
            "classes": {0: "unclassified", 2: "ground", 6: "building"},
        },
        "extraction": {
            "dtm": {"max_triangle_edge_length": 50.0, "thin_factor": 0.1},
            "breaklines": {"min_angle_degrees": 15.0, "min_length_ft": 5.0},
            "contours": {"interval_ft": 1.0, "index_interval_ft": 5.0},
            "planimetrics": {"min_building_area_sqft": 100.0},
        },
        "cad_shielding": {
            "format": "dxf",
            "geometry_healing": {
                "close_tolerance": 0.001,
                "degenerate_face_threshold": 0.0001,
            },
            "layer_mapping": {
                "ground_surface": "TOTaLi-SURV-DTM-DRAFT",
                "breaklines": "TOTaLi-SURV-BRKLN-DRAFT",
                "contours_minor": "TOTaLi-SURV-CONT-MINOR-DRAFT",
                "contours_index": "TOTaLi-SURV-CONT-INDEX-DRAFT",
                "buildings": "TOTaLi-PLAN-BLDG-DRAFT",
            },
            "middleware_timeout_sec": 30,
            "max_retry": 3,
        },
        "linting": {
            "ghost_opacity": 0.4,
            "auto_promote": False,
            "require_pls_signature": True,
        },
        "audit": {
            "log_dir": "audit_logs",
            "log_format": "jsonl",
            "hash_algorithm": "sha256",
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="U4 partner LAS E2E runner")
    parser.add_argument("--output-dir", type=Path, default=Path("partner_las_out"))
    parser.add_argument("--audit-dir", type=Path, default=Path("partner_las_audit"))
    parser.add_argument("--project-id", default="u4_real")
    args = parser.parse_args()

    las_path = os.environ.get("TOTALI_PARTNER_LAS")
    if not las_path or not Path(las_path).is_file():
        print(
            json.dumps({"success": False, "error": "TOTALI_PARTNER_LAS must point at a .las file"}),
            file=sys.stderr,
        )
        return 2

    args.output_dir.mkdir(parents=True, exist_ok=True)
    args.audit_dir.mkdir(parents=True, exist_ok=True)

    audit = AuditLogger(log_dir=str(args.audit_dir), project_id=args.project_id)
    orch = PipelineOrchestrator(_gated_config(), audit, args.output_dir)
    result = orch.run(las_path, phase="all")

    dxf = args.output_dir / "totali_draft_output.dxf"
    manifest_path = args.output_dir / "entity_manifest.json"
    manifest = json.loads(manifest_path.read_text()) if manifest_path.exists() else {}
    ok, errors = verify_log(audit.log_path)

    summary = {
        "success": result.success,
        "phases": [p.phase for p in result.phases],
        "phase_order_ok": [p.phase for p in result.phases] == PHASE_ORDER,
        "dxf_exists": dxf.is_file(),
        "entity_count": len(manifest.get("entities", [])),
        "audit_verify_ok": ok,
        "audit_errors": errors,
        "input_hash": manifest.get("audit_reference", {}).get("input_hash"),
    }
    if not result.success:
        summary["phase_failures"] = [
            {"phase": p.phase, "message": p.message}
            for p in result.phases
            if not p.success
        ]

    print(json.dumps(summary, indent=2))
    return 0 if result.success and ok and dxf.is_file() else 1


if __name__ == "__main__":
    raise SystemExit(main())
