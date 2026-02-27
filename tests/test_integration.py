"""Integration test: full pipeline end-to-end on synthetic data."""
import json
import yaml
import pytest
from pathlib import Path

from totali.pipeline.orchestrator import PipelineOrchestrator
from totali.audit.logger import AuditLogger
from totali.pipeline.models import GeometryStatus


def test_full_pipeline_e2e(config, output_dir, synthetic_las_path):
    audit = AuditLogger(log_dir=str(output_dir / "audit"), project_id="e2e_test")

    pipeline = PipelineOrchestrator(config, audit, output_dir)
    result = pipeline.run(synthetic_las_path, phase="all")

    # All 5 phases succeed
    assert result.success is True
    assert len(result.phases) == 5
    for phase in result.phases:
        assert phase.success is True, f"Phase {phase.phase} failed: {phase.message}"

    # Expected output files exist
    assert (output_dir / "totali_draft_output.dxf").exists()
    assert (output_dir / "entity_manifest.json").exists()
    assert (output_dir / "extraction_report.json").exists()
    assert (output_dir / "lint_report.json").exists()
    assert (output_dir / "review_worksheet.txt").exists()


def test_pipeline_audit_chain_valid(config, output_dir, synthetic_las_path):
    audit = AuditLogger(log_dir=str(output_dir / "audit"), project_id="chain_test")
    pipeline = PipelineOrchestrator(config, audit, output_dir)
    pipeline.run(synthetic_las_path, phase="all")

    valid, errors = audit.verify_chain()
    assert valid is True, f"Audit chain broken: {errors}"
    assert audit.summary()["total_events"] > 0


def test_pipeline_dxf_has_entities(config, output_dir, synthetic_las_path):
    audit = AuditLogger(log_dir=str(output_dir / "audit"), project_id="dxf_test")
    pipeline = PipelineOrchestrator(config, audit, output_dir)
    pipeline.run(synthetic_las_path, phase="all")

    with open(output_dir / "entity_manifest.json") as f:
        manifest = json.load(f)
    assert manifest["entity_count"] > 0
    assert all(e["status"] == "DRAFT" for e in manifest["entities"])


def test_pipeline_lint_report_structure(config, output_dir, synthetic_las_path):
    audit = AuditLogger(log_dir=str(output_dir / "audit"), project_id="lint_test")
    pipeline = PipelineOrchestrator(config, audit, output_dir)
    pipeline.run(synthetic_las_path, phase="all")

    with open(output_dir / "lint_report.json") as f:
        report = json.load(f)
    assert report["auto_promote"] is False
    assert report["require_pls_signature"] is True
    assert report["summary"]["total_items"] > 0
    assert "DRAFT" in report["summary"]["status_counts"]


def test_pipeline_single_phase(config, output_dir, synthetic_las_path):
    audit = AuditLogger(log_dir=str(output_dir / "audit"), project_id="single_test")
    pipeline = PipelineOrchestrator(config, audit, output_dir)
    result = pipeline.run(synthetic_las_path, phase="geodetic")
    assert result.success is True
    assert len(result.phases) == 1
    assert result.phases[0].phase == "geodetic"


def test_pipeline_geodetic_report(config, output_dir, synthetic_las_path):
    audit = AuditLogger(log_dir=str(output_dir / "audit"), project_id="geo_report_test")
    pipeline = PipelineOrchestrator(config, audit, output_dir)
    pipeline.run(synthetic_las_path, phase="all")

    report_files = list(output_dir.glob("*geodetic_report*"))
    assert len(report_files) == 1
    with open(report_files[0]) as f:
        report = json.load(f)
    assert report["crs"]["epsg"] == 2232
    assert report["point_count"] == 3030


def test_pipeline_no_auto_promote_ever(config, output_dir, synthetic_las_path):
    """Verify the pipeline never auto-promotes anything to CERTIFIED."""
    audit = AuditLogger(log_dir=str(output_dir / "audit"), project_id="promote_test")
    pipeline = PipelineOrchestrator(config, audit, output_dir)
    pipeline.run(synthetic_las_path, phase="all")

    with open(output_dir / "entity_manifest.json") as f:
        manifest = json.load(f)
    for entity in manifest["entities"]:
        assert entity["status"] != GeometryStatus.CERTIFIED.value
        assert entity["status"] != GeometryStatus.ACCEPTED.value
