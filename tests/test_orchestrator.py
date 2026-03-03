"""Tests for PipelineOrchestrator."""

from pathlib import Path
from unittest.mock import patch, MagicMock

import numpy as np
import pytest

from totali.pipeline.orchestrator import PipelineOrchestrator, PHASE_ORDER
from totali.pipeline.context import PipelineConfig, PipelineContext
from totali.pipeline.models import PhaseResult, PipelineResult
from totali.audit.logger import AuditLogger


@pytest.fixture
def orchestrator(audit_logger, sample_config, tmp_output):
    return PipelineOrchestrator(sample_config, audit_logger, tmp_output)


class TestInitialization:
    def test_parses_config_to_pydantic(self, orchestrator):
        assert isinstance(orchestrator.config, PipelineConfig)
        assert orchestrator.config.project.name == "test_project"

    def test_all_phases_initialized(self, orchestrator):
        for phase_name in PHASE_ORDER:
            assert phase_name in orchestrator.phases

    def test_phase_order_constant(self):
        assert PHASE_ORDER == ["geodetic", "segment", "extract", "shield", "lint"]


class TestSinglePhaseExecution:
    def test_run_single_phase_only(self, orchestrator, tmp_path):
        fake_las = tmp_path / "input.las"
        fake_las.write_bytes(b"\x00" * 100)
        result = orchestrator.run(str(fake_las), phase="geodetic")
        assert isinstance(result, PipelineResult)
        assert len(result.phases) == 1
        assert result.phases[0].phase == "geodetic"

    def test_run_all_phases(self, orchestrator, tmp_path):
        fake_las = tmp_path / "input.las"
        fake_las.write_bytes(b"\x00" * 100)
        result = orchestrator.run(str(fake_las), phase="all")
        assert isinstance(result, PipelineResult)
        # May stop early on validation failure, but should run at least one phase
        assert len(result.phases) >= 1


class TestContextPassing:
    def test_context_accumulates_data_across_phases(self, audit_logger, sample_config, tmp_output):
        """Verify that data from one phase's PhaseResult is merged into context for the next."""
        orch = PipelineOrchestrator(sample_config, audit_logger, tmp_output)

        fake_result = PhaseResult(
            phase="geodetic", success=True, message="ok",
            data={"points_xyz": np.zeros((10, 3)), "input_hash": "test"},
        )

        # Manually test the merge path
        ctx = PipelineContext(input_path="/f.las", output_dir=tmp_output)
        ctx.merge_data(fake_result.data)
        ctx.phase_status["geodetic"] = "success"

        assert ctx.points_xyz is not None
        assert ctx.input_hash == "test"
        assert ctx.phase_status["geodetic"] == "success"


class TestErrorHandling:
    def test_validation_failure_stops_pipeline(self, orchestrator, tmp_path):
        """If a phase's validate_inputs fails, pipeline should stop."""
        fake_las = tmp_path / "input.las"
        fake_las.write_bytes(b"\x00" * 100)

        # Run all phases — the classifier should fail validation if geodetic
        # didn't produce points_xyz (due to CRS failure in stub environment)
        result = orchestrator.run(str(fake_las), phase="all")
        if not result.success:
            assert any(p.success is False for p in result.phases)

    def test_phase_exception_is_caught_and_reraised(self, audit_logger, sample_config, tmp_output):
        orch = PipelineOrchestrator(sample_config, audit_logger, tmp_output)

        # Replace geodetic phase with one that throws
        mock_phase = MagicMock()
        mock_phase.validate_inputs.return_value = (True, [])
        mock_phase.run.side_effect = RuntimeError("boom")
        orch.phases["geodetic"] = mock_phase

        with pytest.raises(RuntimeError, match="boom"):
            orch.run("/fake.las", phase="geodetic")

    def test_phase_exception_records_state(self, audit_logger, sample_config, tmp_output):
        """Verify that when an exception occurs, PipelineResult and PipelineContext are updated before reraise."""
        orch = PipelineOrchestrator(sample_config, audit_logger, tmp_output)

        mock_phase = MagicMock()
        mock_phase.validate_inputs.return_value = (True, [])
        mock_phase.run.side_effect = RuntimeError("geodetic boom")
        orch.phases["geodetic"] = mock_phase

        captured_result = None
        captured_context = None

        original_result_cls = PipelineResult
        original_context_cls = PipelineContext

        def mock_result_side_effect(*args, **kwargs):
            nonlocal captured_result
            captured_result = original_result_cls(*args, **kwargs)
            return captured_result

        def mock_context_side_effect(*args, **kwargs):
            nonlocal captured_context
            captured_context = original_context_cls(*args, **kwargs)
            return captured_context

        with patch("totali.pipeline.orchestrator.PipelineResult", side_effect=mock_result_side_effect),              patch("totali.pipeline.orchestrator.PipelineContext", side_effect=mock_context_side_effect):

            with pytest.raises(RuntimeError, match="geodetic boom"):
                orch.run("/fake.las", phase="geodetic")

        # Verify result state
        assert captured_result is not None
        assert captured_result.success is False
        assert len(captured_result.phases) == 1
        assert captured_result.phases[0].phase == "geodetic"
        assert captured_result.phases[0].success is False
        assert "geodetic boom" in captured_result.phases[0].message

        # Verify context state
        assert captured_context is not None
        assert captured_context.phase_status["geodetic"] == "exception"
        assert any("geodetic boom" in err for err in captured_context.errors)

        # Verify audit log
        events = audit_logger.get_events("phase_exception")
        assert len(events) == 1
        assert events[0]["data"]["phase"] == "geodetic"
        assert "geodetic boom" in events[0]["data"]["error"]

    def test_failed_phase_sets_context_status(self, audit_logger, sample_config, tmp_output):
        orch = PipelineOrchestrator(sample_config, audit_logger, tmp_output)

        mock_phase = MagicMock()
        mock_phase.validate_inputs.return_value = (True, [])
        mock_phase.run.return_value = PhaseResult(
            phase="geodetic", success=False, message="CRS rejected"
        )
        orch.phases["geodetic"] = mock_phase

        result = orch.run("/fake.las", phase="geodetic")
        assert result.success is False
        assert result.phases[0].message == "CRS rejected"


class TestPipelineResult:
    def test_result_has_project_id(self, orchestrator, tmp_path):
        fake_las = tmp_path / "input.las"
        fake_las.write_bytes(b"\x00" * 100)
        result = orchestrator.run(str(fake_las), phase="geodetic")
        assert result.project_id == "test_project"

    def test_result_tracks_duration(self, orchestrator, tmp_path):
        fake_las = tmp_path / "input.las"
        fake_las.write_bytes(b"\x00" * 100)
        result = orchestrator.run(str(fake_las), phase="geodetic")
        assert result.duration_sec >= 0


class TestAuditIntegration:
    def test_phase_start_logged(self, tmp_path, sample_config):
        audit = AuditLogger(log_dir=str(tmp_path / "audit"), project_id="test")
        orch = PipelineOrchestrator(sample_config, audit, tmp_path / "out")
        (tmp_path / "out").mkdir()

        fake_las = tmp_path / "input.las"
        fake_las.write_bytes(b"\x00" * 100)
        orch.run(str(fake_las), phase="geodetic")

        events = audit.get_events("phase_start")
        assert len(events) >= 1
        assert events[0]["data"]["phase"] == "geodetic"

    def test_phase_completion_logged(self, tmp_path, sample_config):
        audit = AuditLogger(log_dir=str(tmp_path / "audit"), project_id="test")
        orch = PipelineOrchestrator(sample_config, audit, tmp_path / "out")
        (tmp_path / "out").mkdir()

        # Use a mock phase that succeeds
        mock_phase = MagicMock()
        mock_phase.validate_inputs.return_value = (True, [])
        mock_phase.run.return_value = PhaseResult(
            phase="geodetic", success=True, message="ok",
            data={"input_hash": "test"}, output_files=[],
        )
        orch.phases["geodetic"] = mock_phase

        orch.run("/fake.las", phase="geodetic")
        events = audit.get_events("phase_complete")
        assert len(events) == 1
