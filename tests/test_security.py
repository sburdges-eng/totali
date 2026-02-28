import pytest
from click.testing import CliRunner
from unittest.mock import patch, MagicMock
from pathlib import Path
import sys

# Mock heavy dependencies before importing main
sys.modules['laspy'] = MagicMock()
sys.modules['pyproj'] = MagicMock()
sys.modules['pdal'] = MagicMock()
sys.modules['onnxruntime'] = MagicMock()
sys.modules['open3d'] = MagicMock()
sys.modules['triangle'] = MagicMock()
sys.modules['scipy'] = MagicMock()
sys.modules['scipy.spatial'] = MagicMock()
sys.modules['totali.pipeline.orchestrator'] = MagicMock()

from totali.main import main

def test_project_id_validation_success(tmp_path):
    runner = CliRunner()

    # Setup dummy config
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    config_file = config_dir / "pipeline.yaml"
    config_file.write_text("audit:\n  log_dir: audit_logs\n  hash_algorithm: sha256\n")

    # Setup dummy input
    input_file = tmp_path / "input.las"
    input_file.touch()

    with patch('totali.main.AuditLogger') as mock_audit:
        result = runner.invoke(main, [
            "--input", str(input_file),
            "--config", str(config_file),
            "--project-id", "valid_project-123",
            "--dry-run"
        ])

    assert result.exit_code == 0
    assert "Project ID: valid_project-123" in result.output

def test_project_id_validation_failure(tmp_path):
    runner = CliRunner()

    # Setup dummy config
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    config_file = config_dir / "pipeline.yaml"
    config_file.write_text("audit:\n  log_dir: audit_logs\n  hash_algorithm: sha256\n")

    # Setup dummy input
    input_file = tmp_path / "input.las"
    input_file.touch()

    # Bad project_id with path traversal
    bad_project_id = "../evil"

    result = runner.invoke(main, [
        "--input", str(input_file),
        "--config", str(config_file),
        "--project-id", bad_project_id,
        "--dry-run"
    ])

    assert result.exit_code != 0
    assert "Invalid project-id" in result.output
    assert "Only alphanumeric, underscore, and hyphen allowed" in result.output

def test_project_id_validation_failure_special_chars(tmp_path):
    runner = CliRunner()

    # Setup dummy config
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    config_file = config_dir / "pipeline.yaml"
    config_file.write_text("audit:\n  log_dir: audit_logs\n  hash_algorithm: sha256\n")

    # Setup dummy input
    input_file = tmp_path / "input.las"
    input_file.touch()

    # Bad project_id with special chars
    bad_project_id = "project@123"

    result = runner.invoke(main, [
        "--input", str(input_file),
        "--config", str(config_file),
        "--project-id", bad_project_id,
        "--dry-run"
    ])

    assert result.exit_code != 0
    assert "Invalid project-id" in result.output
