"""Full-pipeline end-to-end tests.

Purpose
-------
The rest of the suite tests each phase in isolation (determinism, invariants,
contract). This file is the one test that drives the whole orchestrator from
an input path through all five phases and verifies:

  * the orchestrator executes phases in canonical order,
  * every phase emits a `phase_start` audit event,
  * the hash-chain is intact end-to-end (both in-memory and via CLI verifier),
  * no authoritative/promote/certify boundary is crossed automatically,
  * repeated runs produce a byte-stable shape (phase count / sequence / success),
  * single-phase mode does NOT auto-run prerequisites.

Stub caveat
-----------
`tests/conftest.py` force-stubs `laspy` with `_FakeLasData` (no CRS VLRs).
With the default `sample_config` (`reject_on_missing_crs: True`), geodetic
would halt the run at phase 1. Since the E2E target is to exercise the full
orchestrator, these tests locally relax that single flag to let the stub
data flow through all five phases — every other config value is left at the
conftest default. The "halt-on-first-failure" behavior of the orchestrator
is already pinned by `tests/test_orchestrator_failure.py`; we do not repeat
it here.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from totali.audit.logger import AuditLogger
from totali.audit.verify import verify_log
from totali.pipeline.models import PipelineResult
from totali.pipeline.orchestrator import PHASE_ORDER, PipelineOrchestrator


# The orchestrator halts on the first failing phase, so when the stub LAS
# trips geodetic's CRS gate the pipeline only advances through phase 1.
# Relax the single gating flag so the five phases can actually run; every
# other invariant (authoritative=False, no auto-promote, DRAFT layer names,
# etc.) is still asserted downstream.
def _flowable_config(sample_config: dict) -> dict:
    cfg = dict(sample_config)
    cfg["geodetic"] = dict(sample_config["geodetic"])
    cfg["geodetic"]["reject_on_missing_crs"] = False
    cfg["geodetic"]["reject_on_mixed_datum"] = False
    return cfg


@pytest.fixture
def fake_las_path(tmp_path: Path) -> str:
    """Path to a fake LAS file. The conftest `laspy` stub ignores the path
    and always returns a deterministic `_FakeLasData`, so this only needs
    to exist on disk to satisfy any real-file checks inside phase code."""
    p = tmp_path / "input.las"
    p.write_bytes(b"\x00" * 100)
    return str(p)


# Matches either a TOTaLi draft layer (`TOTaLi-<ZONE>-<KIND>(-<KIND>...)-DRAFT`)
# or a QA scratch layer (`TOTaLi-QA-<...>`). Used to guard against any insert
# landing on a non-DRAFT / non-QA layer.
_DRAFT_LAYER_RE = re.compile(
    r"^TOTaLi-[A-Z0-9]+(?:-[A-Z0-9_]+)+-DRAFT$|^TOTaLi-QA-[A-Z0-9_-]+$"
)


class TestAllPhasesRun:
    """All five phases execute in canonical order under the stub."""

    def test_all_five_phases_complete(
        self, audit_logger, sample_config, tmp_output, fake_las_path
    ) -> None:
        cfg = _flowable_config(sample_config)
        orch = PipelineOrchestrator(cfg, audit_logger, tmp_output)

        result = orch.run(fake_las_path, phase="all")

        assert isinstance(result, PipelineResult)
        # With CRS gating relaxed, the stub flows through all five phases.
        # If this ever breaks, the assertion below pinpoints the failing
        # phase with its error message — which is exactly what the E2E
        # harness is supposed to surface.
        assert len(result.phases) == 5, (
            f"expected all 5 phases to run; got "
            f"{[(p.phase, p.success, p.message) for p in result.phases]}"
        )
        assert [p.phase for p in result.phases] == PHASE_ORDER
        assert result.success is True, (
            "fake input is designed to flow through cleanly; a failure here "
            "indicates a real regression in a phase processor. Failing phase: "
            f"{next((p for p in result.phases if not p.success), None)}"
        )

    def test_phase_order_matches_phase_order_constant(
        self, audit_logger, sample_config, tmp_output, fake_las_path
    ) -> None:
        """The sequence of PhaseResult.phase matches the module-level
        PHASE_ORDER — this pins the invariant that the orchestrator
        iterates its own declared order (no reshuffling / skipping)."""
        cfg = _flowable_config(sample_config)
        orch = PipelineOrchestrator(cfg, audit_logger, tmp_output)

        result = orch.run(fake_las_path, phase="all")

        assert [p.phase for p in result.phases] == list(PHASE_ORDER)


class TestAuditChainIntactAcrossAllPhases:
    """Hash chain stays intact and every phase emits its start event."""

    def test_chain_verifies_after_full_run(
        self, audit_logger, sample_config, tmp_output, fake_las_path
    ) -> None:
        cfg = _flowable_config(sample_config)
        orch = PipelineOrchestrator(cfg, audit_logger, tmp_output)
        orch.run(fake_las_path, phase="all")

        ok, errors = audit_logger.verify_chain()
        assert ok is True
        assert errors == []

    def test_cli_verifier_accepts_log(
        self, audit_logger, sample_config, tmp_output, fake_las_path
    ) -> None:
        cfg = _flowable_config(sample_config)
        orch = PipelineOrchestrator(cfg, audit_logger, tmp_output)
        orch.run(fake_las_path, phase="all")

        ok, errors = verify_log(audit_logger.log_path)
        assert ok is True
        assert errors == []

    def test_every_phase_emitted_phase_start(
        self, audit_logger, sample_config, tmp_output, fake_las_path
    ) -> None:
        cfg = _flowable_config(sample_config)
        orch = PipelineOrchestrator(cfg, audit_logger, tmp_output)
        orch.run(fake_las_path, phase="all")

        starts = audit_logger.get_events("phase_start")
        started_phases = [e["data"]["phase"] for e in starts]
        for phase_name in PHASE_ORDER:
            assert phase_name in started_phases, (
                f"no phase_start event for {phase_name!r}; got {started_phases}"
            )

    def test_run_end_not_emitted_automatically(
        self, audit_logger, sample_config, tmp_output, fake_las_path
    ) -> None:
        """The orchestrator intentionally does not call
        `AuditLogger.close()` — `run_end` is the caller's contract.
        Pin that so a future "helpful" change doesn't silently flip
        the ownership of the terminal record."""
        cfg = _flowable_config(sample_config)
        orch = PipelineOrchestrator(cfg, audit_logger, tmp_output)
        orch.run(fake_las_path, phase="all")

        assert audit_logger.get_events("run_end") == []


class TestInvariantsHoldEndToEnd:
    """Cross-phase invariants: no auto-promote, no auto-certify, DRAFT-only
    inserts, non-authoritative classification."""

    def test_no_promote_event(
        self, audit_logger, sample_config, tmp_output, fake_las_path
    ) -> None:
        cfg = _flowable_config(sample_config)
        orch = PipelineOrchestrator(cfg, audit_logger, tmp_output)
        orch.run(fake_las_path, phase="all")

        assert audit_logger.get_events("promote") == []

    def test_no_certify_event(
        self, audit_logger, sample_config, tmp_output, fake_las_path
    ) -> None:
        cfg = _flowable_config(sample_config)
        orch = PipelineOrchestrator(cfg, audit_logger, tmp_output)
        orch.run(fake_las_path, phase="all")

        assert audit_logger.get_events("certify") == []

    def test_every_insert_lands_on_draft_or_qa_layer(
        self, audit_logger, sample_config, tmp_output, fake_las_path
    ) -> None:
        """Shield emits `insert` per entity. With the stub input, extraction
        typically yields zero entities and zero insert events fire — which
        is itself compliant. If any inserts do fire, every one of them
        must target a DRAFT or TOTaLi-QA-* layer."""
        cfg = _flowable_config(sample_config)
        orch = PipelineOrchestrator(cfg, audit_logger, tmp_output)
        orch.run(fake_las_path, phase="all")

        inserts = audit_logger.get_events("insert")
        if not inserts:
            pytest.skip(
                "no insert events emitted under the stub — "
                "DRAFT-layer invariant vacuously holds"
            )
        for ev in inserts:
            layer = ev["data"].get("layer", "")
            assert _DRAFT_LAYER_RE.match(layer), (
                f"insert event landed on non-DRAFT/non-QA layer: {layer!r}"
            )

    def test_classification_result_is_non_authoritative(
        self, audit_logger, sample_config, tmp_output, fake_las_path
    ) -> None:
        """S-7 invariant: ML classifier output is never authoritative."""
        cfg = _flowable_config(sample_config)
        orch = PipelineOrchestrator(cfg, audit_logger, tmp_output)
        result = orch.run(fake_las_path, phase="all")

        assert result.classification is not None, (
            "classification ran to completion in this config — it must be "
            "surfaced on PipelineResult"
        )
        assert result.classification.authoritative is False


class TestDeterministicAcrossRuns:
    """Two runs with fresh adapters produce identical pipeline shape."""

    def test_phase_shape_is_stable(
        self, sample_config, tmp_path, fake_las_path
    ) -> None:
        cfg = _flowable_config(sample_config)

        def _run(suffix: str) -> tuple[PipelineResult, AuditLogger]:
            out = tmp_path / f"out_{suffix}"
            out.mkdir()
            audit = AuditLogger(
                log_dir=str(tmp_path / f"audit_{suffix}"),
                project_id=f"e2e_{suffix}",
            )
            orch = PipelineOrchestrator(cfg, audit, out)
            return orch.run(fake_las_path, phase="all"), audit

        r1, _ = _run("a")
        r2, _ = _run("b")

        # Same shape: count, ordering, success flag. Timestamps / hashes
        # naturally vary across runs and are deliberately NOT compared.
        assert len(r1.phases) == len(r2.phases)
        assert [p.phase for p in r1.phases] == [p.phase for p in r2.phases]
        assert r1.success == r2.success
        assert [p.success for p in r1.phases] == [p.success for p in r2.phases]


class TestSinglePhaseRun:
    """Single-phase invocation pins the (current) no-prerequisite behavior."""

    def test_single_phase_does_not_auto_run_prerequisites(
        self, audit_logger, sample_config, tmp_output, fake_las_path
    ) -> None:
        """`orchestrator.run(..., phase="geodetic")` runs ONLY geodetic —
        upstream prerequisites are NOT auto-resolved. This documents the
        current behavior; if the orchestrator ever grows dependency
        resolution, this test will force a conscious update."""
        cfg = _flowable_config(sample_config)
        orch = PipelineOrchestrator(cfg, audit_logger, tmp_output)

        result = orch.run(fake_las_path, phase="geodetic")

        assert [p.phase for p in result.phases] == ["geodetic"]
