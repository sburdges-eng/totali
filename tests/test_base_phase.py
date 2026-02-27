"""Tests for the PipelinePhase abstract base class contract."""

import pytest

from totali.pipeline.base_phase import PipelinePhase
from totali.pipeline.context import PipelineContext
from totali.pipeline.models import PhaseResult


class ConcretePhase(PipelinePhase):
    """Minimal concrete implementation for testing the ABC."""
    phase_name = "test_phase"

    def run(self, context: PipelineContext) -> PhaseResult:
        return PhaseResult(phase=self.phase_name, success=True, message="ok")


class MissingRunPhase(PipelinePhase):
    """Omits run() – should not be instantiable."""
    phase_name = "broken"


class TestPipelinePhaseContract:
    def test_concrete_phase_instantiates(self, audit_logger):
        phase = ConcretePhase(config={}, audit=audit_logger)
        assert phase.phase_name == "test_phase"

    def test_abstract_phase_cannot_instantiate(self, audit_logger):
        with pytest.raises(TypeError):
            MissingRunPhase(config={}, audit=audit_logger)

    def test_run_returns_phase_result(self, audit_logger, tmp_output):
        phase = ConcretePhase(config={}, audit=audit_logger)
        ctx = PipelineContext(input_path="/f.las", output_dir=tmp_output)
        result = phase.run(ctx)
        assert isinstance(result, PhaseResult)
        assert result.success is True

    def test_validate_config_default_passes(self, audit_logger):
        phase = ConcretePhase(config={}, audit=audit_logger)
        assert phase.validate_config() is None

    def test_validate_inputs_default_passes(self, audit_logger, tmp_output):
        phase = ConcretePhase(config={}, audit=audit_logger)
        ctx = PipelineContext(input_path="/f.las", output_dir=tmp_output)
        valid, errors = phase.validate_inputs(ctx)
        assert valid is True
        assert errors == []

    def test_get_required_inputs_default_empty(self, audit_logger):
        phase = ConcretePhase(config={}, audit=audit_logger)
        assert phase.get_required_inputs() == set()

    def test_get_provided_outputs_default_empty(self, audit_logger):
        phase = ConcretePhase(config={}, audit=audit_logger)
        assert phase.get_provided_outputs() == set()

    def test_config_stored_on_instance(self, audit_logger):
        cfg = {"key": "value"}
        phase = ConcretePhase(config=cfg, audit=audit_logger)
        assert phase.config == cfg

    def test_audit_logger_stored_on_instance(self, audit_logger):
        phase = ConcretePhase(config={}, audit=audit_logger)
        assert phase.audit is audit_logger
