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

import yaml

from totali.audit.logger import AuditLogger
from totali.audit.verify import verify_log
from totali.pipeline.orchestrator import PHASE_ORDER, PipelineOrchestrator

_REPO = Path(__file__).resolve().parents[1]
_DEFAULT_CONFIG = _REPO / "config" / "pipeline_in_person.yaml"


def _load_config(path: Path) -> dict:
    with open(path, encoding="utf-8") as handle:
        return yaml.safe_load(handle)


def main() -> int:
    parser = argparse.ArgumentParser(description="U4 partner LAS E2E runner")
    parser.add_argument("--output-dir", type=Path, default=Path("partner_las_out"))
    parser.add_argument("--audit-dir", type=Path, default=Path("partner_las_audit"))
    parser.add_argument("--project-id", default="u4_real")
    parser.add_argument(
        "--config",
        type=Path,
        default=_DEFAULT_CONFIG,
        help="Pipeline YAML (default: config/pipeline_in_person.yaml)",
    )
    args = parser.parse_args()

    las_path = os.environ.get("TOTALI_PARTNER_LAS")
    if not las_path or not Path(las_path).is_file():
        print(
            json.dumps({"success": False, "error": "TOTALI_PARTNER_LAS must point at a .las file"}),
            file=sys.stderr,
        )
        return 2

    if not args.config.is_file():
        print(
            json.dumps({"success": False, "error": f"config not found: {args.config}"}),
            file=sys.stderr,
        )
        return 2

    args.output_dir.mkdir(parents=True, exist_ok=True)
    args.audit_dir.mkdir(parents=True, exist_ok=True)

    audit = AuditLogger(log_dir=str(args.audit_dir), project_id=args.project_id)
    orch = PipelineOrchestrator(_load_config(args.config), audit, args.output_dir)
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
            {"phase": p.phase, "message": p.message} for p in result.phases if not p.success
        ]

    print(json.dumps(summary, indent=2))
    return 0 if result.success and ok and dxf.is_file() else 1


if __name__ == "__main__":
    raise SystemExit(main())
