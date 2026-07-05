#!/usr/bin/env python3
"""
Batch ASC smoke harness — runs coded-survey .asc files through the full
TOTaLi pipeline and reports pass/fail + entity/point count per file.

Usage:
    .venv/bin/python tools/batch_asc_smoke.py [path1.asc ...]

Default inputs (3 canonical survey files; skips (1)/(2) duplicates):
    totali/data/TOTaLi/21-034 export.asc
    totali/data/TOTaLi/1-1018 and 9000 OPUS START 220817.asc
    totali/data/TOTaLi/points.asc

Exits 0 if all files pass, non-zero if any fail.
"""

import os
import sys
import time
import traceback
from pathlib import Path

# Repo root = parent of tools/
REPO = Path(__file__).resolve().parent.parent

DEFAULT_INPUTS = [
    REPO / "totali/data/TOTaLi/21-034 export.asc",
    REPO / "totali/data/TOTaLi/1-1018 and 9000 OPUS START 220817.asc",
    REPO / "totali/data/TOTaLi/points.asc",
]

# Field-code library for coded-survey parsing
DEFAULT_FLD = REPO / "totali/data/TOTaLi/BV_BASE_MATCHING_FIELD_CODES.fld"
CONFIG_PATH = REPO / "config/pipeline_in_person.yaml"
SMOKE_OUT = REPO / ".tmp/asc_smoke"


def _entity_count(result) -> int:
    """Best-effort entity/point count from PipelineResult.

    Priority:
      1. extraction.coded_survey_points (coded survey points count)
      2. stats.point_count (raw point cloud count)
      3. healing.input_entity_count (post-extraction entity count)
      4. 0 if nothing available
    """
    if result.extraction is not None:
        pts = getattr(result.extraction, "coded_survey_points", None)
        if pts is not None:
            return len(pts)
    if result.stats is not None:
        pc = getattr(result.stats, "point_count", 0)
        if pc:
            return pc
    if result.healing is not None:
        ec = getattr(result.healing, "input_entity_count", 0)
        if ec:
            return ec
    return 0


def _halted_gate(result) -> str:
    """Return the phase name that halted the pipeline (if any)."""
    for phase_result in result.phases:
        if not phase_result.success:
            return phase_result.phase
    return ""


def _halted_message(result) -> str:
    """Return the message from the first failed phase (if any)."""
    for phase_result in result.phases:
        if not phase_result.success:
            msg = phase_result.message or ""
            # Truncate long messages for table display
            return msg[:120].replace("\n", " ")
    return ""


def run_one(asc_path: Path, cfg: dict, audit_cls, orchestrator_cls) -> dict:
    """Run a single .asc file through the pipeline. Returns a result dict."""
    stem = asc_path.stem.replace(" ", "_").replace("/", "_")
    out_dir = SMOKE_OUT / stem
    audit_dir = out_dir / "audit"
    out_dir.mkdir(parents=True, exist_ok=True)
    audit_dir.mkdir(parents=True, exist_ok=True)

    project_id = f"SMOKE-{stem[:30]}"
    audit = audit_cls(log_dir=str(audit_dir), project_id=project_id)

    t0 = time.time()
    result = orchestrator_cls(cfg, audit, out_dir).run(str(asc_path), phase="all")
    elapsed = time.time() - t0

    return {
        "file": asc_path.name,
        "success": result.success,
        "entity_count": _entity_count(result),
        "halted_gate": _halted_gate(result) if not result.success else "",
        "error": _halted_message(result) if not result.success else "",
        "elapsed_sec": elapsed,
    }


def main(argv=None):
    if argv is None:
        argv = sys.argv[1:]

    # Allow overriding inputs via positional args
    inputs = [Path(p) for p in argv] if argv else DEFAULT_INPUTS

    # Set field-code env if not already set
    if "TOTALI_FIELDCODE_FLD" not in os.environ:
        if DEFAULT_FLD.exists():
            os.environ["TOTALI_FIELDCODE_FLD"] = str(DEFAULT_FLD)
        else:
            print(
                f"WARN: TOTALI_FIELDCODE_FLD not set and default not found: {DEFAULT_FLD}",
                file=sys.stderr,
            )

    # Late imports so env is set before any module reads it
    import yaml
    from totali.audit.logger import AuditLogger
    from totali.pipeline.orchestrator import PipelineOrchestrator

    with open(CONFIG_PATH) as f:
        cfg = yaml.safe_load(f)

    SMOKE_OUT.mkdir(parents=True, exist_ok=True)

    rows = []
    for asc_path in inputs:
        if not asc_path.exists():
            rows.append(
                {
                    "file": asc_path.name,
                    "success": False,
                    "entity_count": 0,
                    "halted_gate": "—",
                    "error": f"File not found: {asc_path}",
                    "elapsed_sec": 0.0,
                }
            )
            continue

        print(f"  Running: {asc_path.name} ...", flush=True)
        try:
            row = run_one(asc_path, cfg, AuditLogger, PipelineOrchestrator)
        except Exception as exc:
            tb = traceback.format_exc()
            rows.append(
                {
                    "file": asc_path.name,
                    "success": False,
                    "entity_count": 0,
                    "halted_gate": "exception",
                    "error": f"{type(exc).__name__}: {exc}",
                    "elapsed_sec": 0.0,
                }
            )
            print(f"  EXCEPTION on {asc_path.name}:\n{tb}", file=sys.stderr)
            continue
        rows.append(row)
        status = "PASS" if row["success"] else "FAIL"
        print(
            f"  {status}  entities={row['entity_count']}  elapsed={row['elapsed_sec']:.1f}s",
            flush=True,
        )

    # Print results table
    print()
    print("=" * 100)
    print(f"{'file':<45} {'ok':>4} {'entities':>10} {'halted_gate':<15} {'error / message'}")
    print("-" * 100)
    for r in rows:
        ok = "YES" if r["success"] else "NO"
        gate = r.get("halted_gate", "") or ""
        err = r.get("error", "") or ""
        # Truncate error for display
        err_disp = err[:38] + "…" if len(err) > 39 else err
        print(f"{r['file']:<45} {ok:>4} {r['entity_count']:>10} {gate:<15} {err_disp}")
    print("=" * 100)

    all_pass = all(r["success"] for r in rows)
    verdict = "ALL PASS" if all_pass else f"{sum(1 for r in rows if not r['success'])} FAIL(S)"
    print(f"\nVerdict: {verdict}  ({len(rows)} files)\n")

    return rows, all_pass


if __name__ == "__main__":
    _, all_pass = main()
    sys.exit(0 if all_pass else 1)
