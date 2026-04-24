"""Reference CLI demonstrating TOTaLi's external-adapter + sanitizer stack.

Exercises the full contract chain:
1. DxfAuracadAdapter reads a DXF via parse_dxf
2. StubL4LInferenceAdapter scores the CAD entities for anomaly
3. StubL4LInferenceAdapter proposes top-3 DRAFT-layer suggestions
4. assemble_prompt_capsule sanitizes a retrieved-context block that
   would otherwise feed an LLM — halt on destructive patterns, audit-emit
   on non-destructive warnings

Usage:
    python -m totali.external.demo [--dxf PATH]

Default DXF: the committed dwg-tool-parser/fixtures/sample.dxf.

This is a reference implementation only. Production callers should use
the adapter classes directly, not this CLI.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Any

from totali.agents.prompt_builder import (
    InjectionHaltError,
    assemble_prompt_capsule,
)
from totali.external.auracad_adapter_dxf import DxfAuracadAdapter
from totali.external.l4l_adapter_stub import StubL4LInferenceAdapter

if TYPE_CHECKING:  # pragma: no cover - typing only
    from totali.audit.logger import AuditLogger


# Default DXF fixture — committed to the repo so the CLI runs against a known,
# deterministic input out of the box. Resolved relative to the repo root at
# ``<root>/dwg-tool-parser/fixtures/sample.dxf``.
_REPO_ROOT = Path(__file__).resolve().parents[2]
_DEFAULT_DXF = _REPO_ROOT / "dwg-tool-parser" / "fixtures" / "sample.dxf"

# Exit codes. Kept distinct from 1/2 (generic/argparse) so operators can
# branch on a detected injection halt specifically.
_EXIT_OK = 0
_EXIT_UNEXPECTED = 1
_EXIT_INJECTION_HALT = 3


def demo(
    dxf_path: Path | None = None,
    audit: "AuditLogger | None" = None,
) -> dict[str, Any]:
    """Run the full external-adapter + sanitizer stack once.

    Deterministic for a given input: the DXF parser, the stub L4L adapter,
    and the sanitizer are all pure-functional at the inputs used here, so
    ``demo(same_path) == demo(same_path)`` across runs and processes.

    Parameters
    ----------
    dxf_path:
        Path to a DXF file. When ``None``, uses the committed fixture at
        ``dwg-tool-parser/fixtures/sample.dxf``.
    audit:
        Optional audit logger forwarded to ``assemble_prompt_capsule``.
        When provided, any sanitizer warning (not just destructive ones)
        emits a ``context_injection_detected`` event through it.

    Returns
    -------
    dict[str, Any]
        Summary dict with keys:

        - ``report_file``: the resolved ``report.file`` string from the
          auracad adapter.
        - ``entity_count``: number of entities parsed.
        - ``anomaly_scores``: list of ``float`` scores, one per entity id,
          in entity-ID order.
        - ``proposal_count``: number of DRAFT-layer proposals returned
          (top_k=3 cap, may be less if the stub returns fewer).
        - ``capsule_warning_count``: total sanitizer warning count across
          the assembled prompt capsule. Zero on the default clean input.

    Notes
    -----
    The retrieved-context block injected into the capsule is intentionally
    clean — no destructive patterns — so the default-path demo succeeds
    end-to-end. Callers that want to exercise the halt path should feed a
    block containing an imperative-destructive or role-override pattern.
    """

    path = dxf_path if dxf_path is not None else _DEFAULT_DXF

    adapter = DxfAuracadAdapter()
    report = adapter.read_cad(str(path))

    l4l = StubL4LInferenceAdapter()
    entity_ids = [e["id"] for e in report.entities]
    scores = l4l.score_anomalies(entity_ids) if entity_ids else []

    layer_names = [lyr["name"] for lyr in report.layers]
    proposals = l4l.propose_actions(
        {"file": report.file, "layers": layer_names},
        top_k=3,
    )

    # Simulated retrieved-context block — intentionally CLEAN so the
    # demo's halt-path is NOT triggered by default. Production callers
    # build these blocks from retrieval results; the shape here mirrors
    # a minimal "CAD summary" artifact.
    capsule, diagnostics = assemble_prompt_capsule(
        [
            (
                "cad_summary",
                f"Parsed {len(entity_ids)} entities from {report.file}",
            ),
        ],
        audit=audit,
    )
    # ``capsule`` is returned by assemble_prompt_capsule but intentionally
    # not exposed in the summary dict — the demo reports counts, not raw
    # prompt text. Referenced here so static analysis sees the binding.
    _ = capsule

    return {
        "report_file": report.file,
        "entity_count": len(report.entities),
        "anomaly_scores": [s.score for s in scores],
        "proposal_count": len(proposals),
        "capsule_warning_count": diagnostics["total_warnings"],
    }


def _build_argparser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m totali.external.demo",
        description=(
            "Reference CLI demonstrating TOTaLi's external-adapter + "
            "sanitizer stack."
        ),
    )
    parser.add_argument(
        "--dxf",
        type=Path,
        default=None,
        help=(
            "Path to a DXF file. Defaults to the committed fixture at "
            "dwg-tool-parser/fixtures/sample.dxf."
        ),
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    """CLI entry point.

    Exit codes:
      * 0 — demo ran to completion (including clean-capsule path).
      * 1 — unexpected error (anything other than a destructive injection).
      * 3 — destructive injection pattern detected; capsule halted.
    """
    parser = _build_argparser()
    args = parser.parse_args(argv)

    try:
        summary = demo(dxf_path=args.dxf)
    except InjectionHaltError as exc:
        print(f"injection halt: {exc}", file=sys.stderr)
        return _EXIT_INJECTION_HALT
    except Exception as exc:  # noqa: BLE001 - reference CLI surface
        print(f"error: {exc}", file=sys.stderr)
        return _EXIT_UNEXPECTED

    # Human-readable summary; stable key ordering for scripted consumers.
    print("TOTaLi external-stack demo — summary")
    for key in (
        "report_file",
        "entity_count",
        "anomaly_scores",
        "proposal_count",
        "capsule_warning_count",
    ):
        print(f"  {key}: {summary[key]}")

    return _EXIT_OK


if __name__ == "__main__":  # pragma: no cover - module CLI
    raise SystemExit(main())
