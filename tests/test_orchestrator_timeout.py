"""P-6: Orchestrator enforces per-phase timeouts.

Resolution order is `<phase>.timeout_sec` → `cad_shielding.middleware_timeout_sec`
→ None. A timeout produces a `phase_timeout` audit event and halts downstream.
"""

import time

import pytest

from totali.audit.logger import AuditLogger
from totali.pipeline.base_phase import PipelinePhase
from totali.pipeline.context import PipelineContext
from totali.pipeline.models import PhaseResult
from totali.pipeline.orchestrator import (
    PHASE_ORDER,
    PhaseTimeout,
    PipelineOrchestrator,
)


class _SlowPhase(PipelinePhase):
    phase_name = "slow"

    def __init__(self, config, audit, sleep_sec):
        super().__init__(config, audit)
        self.sleep_sec = sleep_sec

    def validate_inputs(self, context):
        return True, []

    def run(self, context):
        time.sleep(self.sleep_sec)
        return PhaseResult(phase="slow", success=True, message="ok")


@pytest.fixture
def orchestrator(tmp_output, sample_config):
    audit = AuditLogger(log_dir=str(tmp_output / "audit"), project_id="t")
    orch = PipelineOrchestrator(sample_config, audit, tmp_output)
    return orch, audit


class TestTimeoutResolution:
    def test_explicit_per_phase_overrides(self, orchestrator):
        orch, _ = orchestrator
        orch.config.geodetic["timeout_sec"] = 7.5
        assert orch._phase_timeout("geodetic") == pytest.approx(7.5)

    def test_shield_falls_back_to_middleware(self, orchestrator):
        orch, _ = orchestrator
        # sample_config has cad_shielding.middleware_timeout_sec = 10
        assert orch._phase_timeout("shield") == pytest.approx(10.0)

    def test_no_timeout_when_unset(self, orchestrator):
        orch, _ = orchestrator
        assert orch._phase_timeout("geodetic") is None
        assert orch._phase_timeout("lint") is None


class TestRunWithTimeout:
    def test_returns_quickly_when_phase_fast(self, orchestrator):
        orch, _ = orchestrator
        slow = _SlowPhase({}, orch.audit, sleep_sec=0.01)
        ctx = PipelineContext(input_path="/x", output_dir=orch.output_dir)
        result = orch._run_with_timeout(slow, ctx, timeout=1.0)
        assert result.success is True

    def test_raises_phase_timeout_when_exceeded(self, orchestrator):
        orch, _ = orchestrator
        slow = _SlowPhase({}, orch.audit, sleep_sec=0.5)
        ctx = PipelineContext(input_path="/x", output_dir=orch.output_dir)
        with pytest.raises(PhaseTimeout):
            orch._run_with_timeout(slow, ctx, timeout=0.05)

    def test_no_timeout_path_runs_directly(self, orchestrator):
        orch, _ = orchestrator
        slow = _SlowPhase({}, orch.audit, sleep_sec=0.01)
        ctx = PipelineContext(input_path="/x", output_dir=orch.output_dir)
        result = orch._run_with_timeout(slow, ctx, timeout=None)
        assert result.success is True


class TestEndToEndTimeout:
    def test_timeout_halts_pipeline_and_emits_audit(self, orchestrator, tmp_path):
        orch, audit = orchestrator
        # Replace geodetic phase with a sleeper and set its timeout to 0.05s.
        orch.config.geodetic["timeout_sec"] = 0.05
        orch.phases["geodetic"] = _SlowPhase({}, audit, sleep_sec=0.4)

        fake_las = tmp_path / "input.las"
        fake_las.write_bytes(b"\x00" * 100)

        result = orch.run(str(fake_las), phase="all")
        assert result.success is False
        assert any(p.phase == "geodetic" and not p.success for p in result.phases)
        # Subsequent phases must not run.
        ran_phases = [p.phase for p in result.phases]
        assert ran_phases == ["geodetic"], f"only geodetic should run, got {ran_phases}"

        timeout_events = audit.get_events("phase_timeout")
        assert timeout_events, "phase_timeout audit event must be emitted"
        payload = timeout_events[0]["data"]
        assert payload["phase"] == "geodetic"
        assert payload["timeout_sec"] == pytest.approx(0.05)


class TestPhaseOrderUnchanged:
    def test_phase_order_constant(self):
        assert PHASE_ORDER == ["geodetic", "segment", "extract", "shield", "lint"]
