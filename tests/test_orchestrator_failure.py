"""P-4: orchestrator failure propagation.

validate_inputs returning False halts downstream. A failing run() result also
halts downstream. Both cases produce a recorded PhaseResult and a phase_failed
audit event.
"""

import pytest

from totali.audit.logger import AuditLogger
from totali.pipeline.base_phase import PipelinePhase
from totali.pipeline.models import PhaseResult
from totali.pipeline.orchestrator import PipelineOrchestrator


class _FailValidate(PipelinePhase):
    phase_name = "failv"

    def validate_inputs(self, context):
        return False, ["synthetic validation error"]

    def run(self, context):
        raise AssertionError("run must not be called when validate_inputs fails")


class _FailRun(PipelinePhase):
    phase_name = "failr"

    def validate_inputs(self, context):
        return True, []

    def run(self, context):
        return PhaseResult(phase="failr", success=False, message="deliberate fail")


@pytest.fixture
def orchestrator(tmp_output, sample_config):
    audit = AuditLogger(log_dir=str(tmp_output / "audit"), project_id="fp")
    orch = PipelineOrchestrator(sample_config, audit, tmp_output)
    return orch, audit


class TestValidateInputsFailure:
    def test_validation_failure_halts_pipeline(self, orchestrator, tmp_path):
        orch, audit = orchestrator
        orch.phases["geodetic"] = _FailValidate({}, audit)

        fake_las = tmp_path / "input.las"
        fake_las.write_bytes(b"\x00" * 100)

        result = orch.run(str(fake_las), phase="all")
        assert result.success is False
        assert len(result.phases) == 1
        assert result.phases[0].phase == "geodetic"
        assert result.phases[0].success is False
        assert "validation" in result.phases[0].message.lower()

    def test_phase_failed_audit_event_on_validation_failure(self, orchestrator, tmp_path):
        orch, audit = orchestrator
        orch.phases["geodetic"] = _FailValidate({}, audit)

        fake_las = tmp_path / "input.las"
        fake_las.write_bytes(b"\x00" * 100)
        orch.run(str(fake_las), phase="all")

        failed = audit.get_events("phase_failed")
        assert failed, "phase_failed must be emitted on validation failure"
        assert failed[0]["data"]["phase"] == "geodetic"


class TestRunFailure:
    def test_run_failure_halts_pipeline(self, orchestrator, tmp_path):
        orch, audit = orchestrator
        orch.phases["geodetic"] = _FailRun({}, audit)

        fake_las = tmp_path / "input.las"
        fake_las.write_bytes(b"\x00" * 100)
        result = orch.run(str(fake_las), phase="all")

        assert result.success is False
        assert result.phases[-1].success is False
        # No downstream phases ran (only the geodetic-slot fake phase).
        assert len(result.phases) == 1

    def test_phase_failed_audit_event_on_run_failure(self, orchestrator, tmp_path):
        orch, audit = orchestrator
        orch.phases["geodetic"] = _FailRun({}, audit)

        fake_las = tmp_path / "input.las"
        fake_las.write_bytes(b"\x00" * 100)
        orch.run(str(fake_las), phase="all")

        failed = audit.get_events("phase_failed")
        assert failed
        assert any(e["data"]["phase"] == "geodetic" for e in failed)


class TestPhaseResultStructure:
    def test_failed_phase_result_has_duration(self, orchestrator, tmp_path):
        orch, audit = orchestrator
        orch.phases["geodetic"] = _FailRun({}, audit)

        fake_las = tmp_path / "input.las"
        fake_las.write_bytes(b"\x00" * 100)
        result = orch.run(str(fake_las), phase="all")

        assert result.phases[0].duration_sec is not None
        assert result.phases[0].duration_sec >= 0.0
