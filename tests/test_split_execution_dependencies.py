from totali.audit.logger import AuditLogger
from totali.pipeline.orchestrator import PipelineOrchestrator


def test_segment_phase_requires_geodetic_context(sample_config, tmp_path):
    output_dir = tmp_path / "output"
    output_dir.mkdir(parents=True, exist_ok=True)

    audit = AuditLogger(log_dir=str(output_dir / "audit"), project_id="test_project")
    orchestrator = PipelineOrchestrator(sample_config, audit, output_dir)

    input_file = output_dir / "input.las"
    input_file.write_bytes(b"fake-las-content")

    result = orchestrator.run(str(input_file), phase="segment")

    assert result.success is False
    assert result.phases
    assert result.phases[0].phase == "segment"
    assert "run geodetic phase first" in result.phases[0].message
