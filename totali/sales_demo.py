"""M1 seven-step sales demo — survey→CAD Key Flow (commercial v1).

Exercises the sellable vertical end-to-end:

1. Import raw LiDAR/drone point cloud
2. Geodetic gatekeeper validates CRS/datum/units (pass or quarantine)
3. AI classifies (advisory) → deterministic extraction (authoritative)
4. Surveyor review / lint (DRAFT until human certification — no auto-promote)
5. Export defensible DXF + audit record
6. Hand to civil client (narrative checkpoint)
7. Show timing + independently verifiable provenance

Usage::

    python -m totali.sales_demo
    python -m totali.sales_demo --input partner.las --manual-baseline-sec 14400
    TOTALI_PARTNER_LAS=/path/to/job.las python -m totali.sales_demo

Default LAS: ``tests/fixtures/survey_corpus/synth_topo.las`` (deterministic
synthetic topo for dry-run demos). Set ``TOTALI_PARTNER_LAS`` or ``--input``
for the design-partner dataset (M1 U4).
"""

from __future__ import annotations

import argparse
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from totali.audit.logger import AuditLogger
from totali.audit.verify import verify_log
from totali.pipeline.orchestrator import PipelineOrchestrator

_REPO_ROOT = Path(__file__).resolve().parents[1]
_DEFAULT_LAS = _REPO_ROOT / "tests" / "fixtures" / "survey_corpus" / "synth_topo.las"
_DEFAULT_CONFIG = _REPO_ROOT / "config" / "pipeline.yaml"

_EXIT_OK = 0
_EXIT_UNEXPECTED = 1


@dataclass(frozen=True)
class SalesStep:
    number: str
    title: str
    detail: str


SALES_STEPS: tuple[SalesStep, ...] = (
    SalesStep("1", "Import raw LiDAR", "Ingest topo point cloud (.las/.laz)."),
    SalesStep(
        "2",
        "Geodetic gatekeeper",
        "Validate CRS, datum, units; quarantine ambiguous cases.",
    ),
    SalesStep(
        "3",
        "Classify + extract",
        "AI labels are advisory; TIN/contours/breaklines are deterministic.",
    ),
    SalesStep(
        "4",
        "Surveyor review",
        "Lint runs; output stays DRAFT until PLS certification (no auto-promote).",
    ),
    SalesStep("5", "Export DXF + audit", "Defensible DXF and SHA-256 audit chain."),
    SalesStep("6", "Deliver to client", "Hand off for civil design (Civil 3D / Carlson)."),
    SalesStep(
        "7",
        "Prove speed + provenance",
        "Wall-clock metrics vs optional manual baseline; verify audit chain.",
    ),
)


def _resolve_input(explicit: Path | None) -> Path:
    if explicit is not None:
        return explicit
    env = os.environ.get("TOTALI_PARTNER_LAS")
    if env:
        return Path(env)
    return _DEFAULT_LAS


def _load_config(config_path: Path) -> dict[str, Any]:
    with open(config_path, encoding="utf-8") as handle:
        return yaml.safe_load(handle)


def _print_step(step: SalesStep, status: str) -> None:
    print(f"[{step.number}/7] {step.title} — {status}")
    print(f"       {step.detail}")


def demo(
    *,
    input_path: Path | None = None,
    config_path: Path | None = None,
    output_dir: Path | None = None,
    project_id: str = "SALES-DEMO",
    manual_baseline_sec: float | None = None,
    audit: AuditLogger | None = None,
) -> dict[str, Any]:
    """Run the seven-step sales flow; return a summary dict for tests/CI."""
    las = _resolve_input(input_path)
    if not las.is_file():
        raise FileNotFoundError(f"input LAS not found: {las}")

    cfg_path = config_path or _DEFAULT_CONFIG
    config = _load_config(cfg_path)

    out = output_dir or (_REPO_ROOT / "output" / "sales_demo")
    out.mkdir(parents=True, exist_ok=True)

    _print_step(SALES_STEPS[0], f"input={las.name}")

    raw_log_dir = config.get("audit", {}).get("log_dir", "audit_logs")
    log_dir = Path(raw_log_dir)
    if not log_dir.is_absolute():
        log_dir = out / "audit"

    audit_logger = audit or AuditLogger(
        log_dir=str(log_dir),
        project_id=project_id,
    )

    _print_step(SALES_STEPS[1], "starting pipeline (geodetic gates active)")
    _print_step(SALES_STEPS[2], "classify + extract phases follow geodetic")

    pipeline = PipelineOrchestrator(config, audit_logger, out)
    result = pipeline.run(
        str(las),
        phase="all",
        manual_baseline_sec=manual_baseline_sec,
    )

    if not result.success:
        phases = [(p.phase, p.success, p.message) for p in result.phases]
        raise RuntimeError(f"pipeline failed: {phases}")

    dxf = out / "totali_draft_output.dxf"
    manifest = out / "entity_manifest.json"
    _print_step(SALES_STEPS[3], "lint complete — DRAFT until PLS certification")
    _print_step(SALES_STEPS[4], f"DXF={'yes' if dxf.exists() else 'missing'}")

    ok, errors = verify_log(audit_logger.log_path)
    _print_step(SALES_STEPS[5], "ready for civil client handoff")
    _print_step(
        SALES_STEPS[6],
        f"audit_verify={'PASS' if ok else 'FAIL'}; duration={result.duration_sec:.2f}s",
    )

    metrics_path = Path(audit_logger.log_path).with_suffix(".metrics.json")
    summary: dict[str, Any] = {
        "input": str(las),
        "project_id": project_id,
        "phase_count": len(result.phases),
        "success": result.success,
        "dxf_path": str(dxf) if dxf.exists() else None,
        "manifest_path": str(manifest) if manifest.exists() else None,
        "audit_log": str(audit_logger.log_path),
        "audit_verify_ok": ok,
        "audit_verify_errors": errors,
        "total_duration_sec": result.duration_sec,
        "metrics_path": str(metrics_path) if metrics_path.exists() else None,
        "manual_baseline_sec": manual_baseline_sec,
        "classification_authoritative": (
            result.classification.authoritative if result.classification is not None else None
        ),
    }

    return summary


def _build_argparser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m totali.sales_demo",
        description="M1 seven-step survey→CAD sales demo (Key Flow).",
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=None,
        help="Topo LAS/LAZ (default: synth_topo fixture or TOTALI_PARTNER_LAS)",
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=_DEFAULT_CONFIG,
        help="Pipeline config YAML",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Output directory (default: output/sales_demo)",
    )
    parser.add_argument(
        "--project-id",
        default="SALES-DEMO",
        help="Audit project identifier",
    )
    parser.add_argument(
        "--manual-baseline-sec",
        type=float,
        default=None,
        help="Optional manual drafting baseline in seconds (SC2; no %% computed)",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_argparser().parse_args(argv)
    try:
        summary = demo(
            input_path=args.input,
            config_path=args.config,
            output_dir=args.output,
            project_id=args.project_id,
            manual_baseline_sec=args.manual_baseline_sec,
        )
    except Exception as exc:  # noqa: BLE001 - CLI surface
        print(f"error: {exc}", file=sys.stderr)
        return _EXIT_UNEXPECTED

    print("\nTOTaLi sales demo — summary")
    for key in (
        "input",
        "project_id",
        "dxf_path",
        "audit_log",
        "audit_verify_ok",
        "total_duration_sec",
        "metrics_path",
        "manual_baseline_sec",
        "classification_authoritative",
    ):
        print(f"  {key}: {summary[key]}")

    if not summary["audit_verify_ok"]:
        return _EXIT_UNEXPECTED
    return _EXIT_OK


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
