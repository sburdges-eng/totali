import pytest
from click.testing import CliRunner
from unittest.mock import patch, MagicMock
from pathlib import Path
import yaml
import sys

from totali.main import main
from totali.pipeline.models import PipelineResult

@pytest.fixture
def runner():
    return CliRunner()

@pytest.fixture
def dummy_input(tmp_path):
    p = tmp_path / "input.las"
    p.write_bytes(b"\x00" * 100)
    return str(p)

@pytest.fixture
def dummy_config(tmp_path, sample_config):
    p = tmp_path / "config.yaml"
    with open(p, "w") as f:
        yaml.dump(sample_config, f)
    return str(p)

def test_main_success(runner, dummy_input, dummy_config, tmp_path):
    with patch("totali.main.PipelineOrchestrator") as MockOrch:
        mock_orch_instance = MockOrch.return_value
        mock_orch_instance.run.return_value = PipelineResult(
            project_id="test_project",
            success=True,
            output_files=[Path("out.dxf")]
        )

        result = runner.invoke(main, ["--input", dummy_input, "--config", dummy_config, "--output", str(tmp_path / "out")])

        assert result.exit_code == 0
        assert "Pipeline complete" in result.output

def test_main_error_handling(runner, dummy_input, dummy_config, tmp_path):
    with patch("totali.main.PipelineOrchestrator") as MockOrch:
        mock_orch_instance = MockOrch.return_value
        mock_orch_instance.run.side_effect = Exception("test pipeline failure")

        # We also need to mock AuditLogger to verify it's called
        with patch("totali.main.AuditLogger") as MockAudit:
            mock_audit_instance = MockAudit.return_value

            result = runner.invoke(main, ["--input", dummy_input, "--config", dummy_config, "--output", str(tmp_path / "out")])

            assert result.exit_code == 1
            assert "Pipeline failed: test pipeline failure" in result.output

            # Verify audit.log was called with pipeline_error
            mock_audit_instance.log.assert_any_call("pipeline_error", {"error": "test pipeline failure", "phase": "all"})

def test_main_dry_run(runner, dummy_input, dummy_config, tmp_path):
    with patch("totali.main.PipelineOrchestrator") as MockOrch:
        result = runner.invoke(main, ["--input", dummy_input, "--config", dummy_config, "--dry-run"])

        assert result.exit_code == 0
        assert "[DRY RUN] Config valid" in result.output
        MockOrch.assert_not_called()
