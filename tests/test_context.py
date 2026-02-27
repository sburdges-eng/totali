"""Tests for PipelineContext and config Pydantic models."""

import numpy as np
import pytest
from pydantic import ValidationError

from totali.pipeline.context import (
    AuditConfig,
    PipelineConfig,
    PipelineContext,
    ProjectConfig,
)
from totali.pipeline.models import (
    CRSMetadata,
    ClassificationResult,
    ExtractionResult,
    HealingReport,
    LintItem,
    PointCloudStats,
)


class TestProjectConfig:
    def test_defaults(self):
        cfg = ProjectConfig()
        assert cfg.name == "unknown"
        assert cfg.version == "0.1.0"
        assert cfg.pls_authority is None

    def test_custom_values(self):
        cfg = ProjectConfig(name="MyProject", version="2.0.0", pls_authority="CO #1234")
        assert cfg.name == "MyProject"
        assert cfg.pls_authority == "CO #1234"


class TestAuditConfig:
    def test_defaults(self):
        cfg = AuditConfig()
        assert cfg.log_dir == "audit_logs"
        assert cfg.hash_algorithm == "sha256"
        assert cfg.log_events == []

    def test_custom_events(self):
        cfg = AuditConfig(log_events=["ingest", "certify"])
        assert len(cfg.log_events) == 2


class TestPipelineConfig:
    def test_from_minimal_dict(self):
        cfg = PipelineConfig.model_validate({})
        assert cfg.project.name == "unknown"
        assert cfg.geodetic == {}
        assert cfg.audit.log_dir == "audit_logs"

    def test_from_full_dict(self, sample_config):
        cfg = PipelineConfig.model_validate(sample_config)
        assert cfg.project.name == "test_project"
        assert "allowed_crs" in cfg.geodetic
        assert cfg.segmentation["device"] == "cpu"
        assert cfg.audit.hash_algorithm == "sha256"

    def test_extra_keys_allowed(self):
        cfg = PipelineConfig.model_validate({"custom_field": "ok"})
        assert cfg.custom_field == "ok"


class TestPipelineContext:
    def test_minimal_creation(self, tmp_output):
        ctx = PipelineContext(input_path="/some/file.las", output_dir=tmp_output)
        assert ctx.points_xyz is None
        assert ctx.classification is None
        assert ctx.phase_status == {}
        assert ctx.errors == []

    def test_merge_data_known_fields(self, tmp_output):
        ctx = PipelineContext(input_path="/f.las", output_dir=tmp_output)
        pts = np.array([[1, 2, 3], [4, 5, 6]])
        ctx.merge_data({"points_xyz": pts, "input_hash": "deadbeef"})
        assert ctx.input_hash == "deadbeef"
        np.testing.assert_array_equal(ctx.points_xyz, pts)

    def test_merge_data_unknown_keys_go_to_extras(self, tmp_output):
        ctx = PipelineContext(input_path="/f.las", output_dir=tmp_output)
        ctx.merge_data({"totally_new_field": 42})
        assert ctx.extras["totally_new_field"] == 42

    def test_merge_data_preserves_existing(self, tmp_output):
        ctx = PipelineContext(input_path="/f.las", output_dir=tmp_output, input_hash="first")
        ctx.merge_data({"input_hash": "second"})
        assert ctx.input_hash == "second"

    def test_numpy_arrays_accepted(self, tmp_output):
        ctx = PipelineContext(
            input_path="/f.las",
            output_dir=tmp_output,
            points_xyz=np.zeros((10, 3)),
        )
        assert ctx.points_xyz.shape == (10, 3)

    def test_phase_status_tracking(self, tmp_output):
        ctx = PipelineContext(input_path="/f.las", output_dir=tmp_output)
        ctx.phase_status["geodetic"] = "success"
        ctx.phase_status["segment"] = "failed"
        assert ctx.phase_status["geodetic"] == "success"

    def test_errors_accumulate(self, tmp_output):
        ctx = PipelineContext(input_path="/f.las", output_dir=tmp_output)
        ctx.errors.append("err1")
        ctx.errors.append("err2")
        assert len(ctx.errors) == 2

    def test_classification_round_trip(self, tmp_output, sample_classification):
        ctx = PipelineContext(input_path="/f.las", output_dir=tmp_output)
        ctx.merge_data({"classification": sample_classification})
        assert ctx.classification is sample_classification
        assert ctx.classification.mean_confidence > 0

    def test_lint_items_default_empty(self, tmp_output):
        ctx = PipelineContext(input_path="/f.las", output_dir=tmp_output)
        assert ctx.lint_items == []
