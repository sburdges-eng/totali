"""Tests for Phase 5: SurveyorLinter."""

from datetime import datetime
from pathlib import Path

import numpy as np
import pytest

from totali.linting.surveyor_lint import SurveyorLinter
from totali.pipeline.context import PipelineContext
from totali.pipeline.models import (
    PhaseResult,
    LintItem,
    GeometryStatus,
    OcclusionType,
    ExtractionResult,
    ClassificationResult,
    CRSMetadata,
    PointCloudStats,
)


@pytest.fixture
def linter(audit_logger, sample_config):
    return SurveyorLinter(sample_config["linting"], audit_logger)


@pytest.fixture
def sample_manifest():
    """Entity manifest as would be produced by CADShield."""
    return {
        "format": "dxf",
        "path": "/fake/output.dxf",
        "entity_count": 3,
        "entities": [
            {"id": "ent001", "type": "3DFACE", "layer": "TOTaLi-SURV-DTM-DRAFT", "status": "DRAFT", "source_hash": "aabb"},
            {"id": "ent002", "type": "POLYLINE", "layer": "TOTaLi-SURV-BRKLN-DRAFT", "status": "DRAFT", "source_hash": "ccdd"},
            {"id": "ent003", "type": "POLYGON", "layer": "TOTaLi-PLAN-BLDG-DRAFT", "status": "DRAFT", "source_hash": "eeff"},
        ],
    }


class TestAutoPromoteHardcoded:
    def test_auto_promote_always_false(self, linter):
        assert linter.auto_promote is False

    def test_config_cannot_enable_auto_promote(self, audit_logger):
        linter = SurveyorLinter({"auto_promote": True}, audit_logger)
        assert linter.auto_promote is False


class TestValidateInputs:
    def test_missing_manifest(self, linter, tmp_output):
        ctx = PipelineContext(input_path="/f.las", output_dir=tmp_output)
        valid, errors = linter.validate_inputs(ctx)
        assert valid is False
        assert any("manifest" in e for e in errors)

    def test_valid_with_manifest(self, linter, tmp_output, sample_manifest):
        ctx = PipelineContext(
            input_path="/f.las", output_dir=tmp_output,
            manifest=sample_manifest,
        )
        valid, errors = linter.validate_inputs(ctx)
        assert valid is True


class TestConfidenceEstimation:
    def test_no_classification_returns_default(self, linter):
        entity = {"id": "x", "type": "LINE", "layer": "L"}
        conf = linter._estimate_confidence(entity, None)
        assert conf == 0.5

    def test_uses_mean_confidence(self, linter, sample_classification):
        entity = {"id": "x", "type": "LINE", "layer": "L"}
        conf = linter._estimate_confidence(entity, sample_classification)
        assert conf == sample_classification.mean_confidence


class TestOcclusionCheck:
    def test_no_extraction_returns_none(self, linter):
        entity = {"id": "x", "layer": "L"}
        result = linter._check_occlusion(entity, None)
        assert result == OcclusionType.NONE

    def test_occlusion_layer_returns_unknown(self, linter):
        entity = {"id": "x", "layer": "TOTaLi-QA-OCCLUSION"}
        ext = ExtractionResult(occlusion_zones=[np.array([[0, 0], [1, 1], [0, 1]])])
        result = linter._check_occlusion(entity, ext)
        assert result == OcclusionType.UNKNOWN


class TestLintReport:
    def test_report_structure(self, linter, sample_classification):
        items = [
            LintItem(item_id="a", geometry_type="LINE", layer="L", confidence=0.9),
            LintItem(item_id="b", geometry_type="FACE", layer="L", confidence=0.3),
        ]
        ext = ExtractionResult(qa_flags=[{"type": "test", "severity": "info", "message": "ok"}])
        report = linter._generate_lint_report(items, ext)

        assert "generated" in report
        assert report["auto_promote"] is False
        assert report["summary"]["total_items"] == 2
        assert report["summary"]["high_confidence"] == 1
        assert report["summary"]["low_confidence"] == 1
        assert len(report["items"]) == 2
        assert len(report["qa_flags"]) == 1

    def test_certification_requirements_present(self, linter):
        report = linter._generate_lint_report([], None)
        reqs = report["certification_requirements"]
        assert reqs["pls_signature_required"] is True
        assert reqs["no_draft_items_allowed_in_final"] is True


class TestConfidenceColor:
    def test_high_confidence_green(self, linter):
        assert linter._confidence_color(0.80) == "#00FF00"

    def test_medium_confidence_amber(self, linter):
        assert linter._confidence_color(0.60) == "#FFAA00"

    def test_low_confidence_red(self, linter):
        assert linter._confidence_color(0.30) == "#FF0000"


class TestReviewWorksheet:
    def test_writes_worksheet_file(self, linter, tmp_path, sample_classification):
        items = [
            LintItem(item_id="a", geometry_type="LINE", layer="L", confidence=0.3),
        ]
        ext = ExtractionResult(
            qa_flags=[{"type": "low_confidence", "severity": "warning", "message": "test"}]
        )
        ws_path = tmp_path / "worksheet.txt"
        linter._write_review_worksheet(items, ext, ws_path)

        assert ws_path.exists()
        content = ws_path.read_text()
        assert "SURVEYOR REVIEW WORKSHEET" in content
        assert "PLS Signature" in content
        assert "ACCEPT" in content
        assert "REJECT" in content

    def test_worksheet_contains_attention_items(self, linter, tmp_path):
        items = [
            LintItem(item_id="low", geometry_type="LINE", layer="L", confidence=0.2),
            LintItem(item_id="high", geometry_type="LINE", layer="L", confidence=0.9),
        ]
        ws_path = tmp_path / "ws.txt"
        linter._write_review_worksheet(items, None, ws_path)
        content = ws_path.read_text()
        assert "low" in content
        assert "ITEMS REQUIRING ATTENTION" in content


class TestInteractiveReview:
    def test_accept_item(self, audit_logger):
        item = LintItem(item_id="x", geometry_type="LINE", layer="L")
        SurveyorLinter.accept_item(item, "John PLS", audit_logger, "looks good")
        assert item.status == GeometryStatus.ACCEPTED
        assert item.reviewer == "John PLS"
        assert item.review_timestamp is not None
        assert item.notes == "looks good"

    def test_reject_item(self, audit_logger):
        item = LintItem(item_id="x", geometry_type="LINE", layer="L")
        SurveyorLinter.reject_item(item, "Jane PLS", audit_logger, "bad geometry")
        assert item.status == GeometryStatus.REJECTED
        assert item.reviewer == "Jane PLS"

    def test_promote_blocked_with_draft_items(self, audit_logger):
        items = [
            LintItem(item_id="a", geometry_type="LINE", layer="L-DRAFT", status=GeometryStatus.ACCEPTED),
            LintItem(item_id="b", geometry_type="LINE", layer="L-DRAFT", status=GeometryStatus.DRAFT),
        ]
        result = SurveyorLinter.promote_to_certified(items, "PLS Name", "12345", audit_logger)
        assert result is False

    def test_promote_succeeds_when_all_reviewed(self, audit_logger):
        items = [
            LintItem(item_id="a", geometry_type="LINE", layer="L-DRAFT", status=GeometryStatus.ACCEPTED),
            LintItem(item_id="b", geometry_type="LINE", layer="L-DRAFT", status=GeometryStatus.REJECTED),
        ]
        result = SurveyorLinter.promote_to_certified(items, "PLS Name", "12345", audit_logger)
        assert result is True
        assert items[0].status == GeometryStatus.CERTIFIED
        assert items[0].layer == "L"  # -DRAFT removed
        assert items[1].status == GeometryStatus.REJECTED  # unchanged

    def test_promote_removes_draft_suffix(self, audit_logger):
        items = [
            LintItem(item_id="a", geometry_type="LINE", layer="TOTaLi-SURV-DTM-DRAFT", status=GeometryStatus.ACCEPTED),
        ]
        SurveyorLinter.promote_to_certified(items, "P", "1", audit_logger)
        assert items[0].layer == "TOTaLi-SURV-DTM"


class TestPhaseRun:
    def test_run_produces_lint_report(self, linter, tmp_output, sample_manifest, sample_classification):
        ext = ExtractionResult(qa_flags=[])
        ctx = PipelineContext(
            input_path="/f.las", output_dir=tmp_output,
            manifest=sample_manifest,
            extraction=ext,
            classification=sample_classification,
        )
        result = linter.run(ctx)
        assert isinstance(result, PhaseResult)
        assert result.phase == "lint"
        assert result.success is True
        assert "lint_items" in result.data
        assert "lint_report" in result.data
        assert len(result.output_files) == 2

    def test_output_files_created(self, linter, tmp_output, sample_manifest, sample_classification):
        ctx = PipelineContext(
            input_path="/f.las", output_dir=tmp_output,
            manifest=sample_manifest,
            extraction=ExtractionResult(),
            classification=sample_classification,
        )
        linter.run(ctx)
        assert (tmp_output / "lint_report.json").exists()
        assert (tmp_output / "review_worksheet.txt").exists()

    def test_all_items_start_as_draft(self, linter, tmp_output, sample_manifest, sample_classification):
        ctx = PipelineContext(
            input_path="/f.las", output_dir=tmp_output,
            manifest=sample_manifest,
            classification=sample_classification,
        )
        result = linter.run(ctx)
        for item in result.data["lint_items"]:
            assert item.status == GeometryStatus.DRAFT

    def test_run_entity_missing_layer_does_not_raise(self, linter, tmp_output, sample_classification):
        """H1: Manifest entity missing 'layer' should not KeyError; use default."""
        manifest = {
            "entities": [
                {"id": "e1", "type": "LINE"},
                {"id": "e2", "type": "FACE", "layer": "L2"},
            ],
        }
        ctx = PipelineContext(
            input_path="/f.las", output_dir=tmp_output,
            manifest=manifest,
            extraction=ExtractionResult(),
            classification=sample_classification,
        )
        result = linter.run(ctx)
        assert result.success is True
        assert len(result.data["lint_items"]) == 2
        assert result.data["lint_items"][0].layer == ""
        assert result.data["lint_items"][1].layer == "L2"

    def test_worksheet_qa_flag_missing_severity_does_not_raise(self, linter, tmp_path):
        """H2: qa_flag missing 'severity' or 'message' should not KeyError."""
        items = [LintItem(item_id="a", geometry_type="LINE", layer="L", confidence=0.5)]
        ext = ExtractionResult(
            qa_flags=[
                {"message": "only message"},
                {"severity": "warning"},
                {"type": "x", "severity": "info", "message": "full"},
            ]
        )
        ws_path = tmp_path / "ws.txt"
        linter._write_review_worksheet(items, ext, ws_path)
        assert ws_path.exists()
        content = ws_path.read_text()
        assert "only message" in content or "full" in content

    def test_promote_short_layer_name_removes_draft_suffix(self, audit_logger):
        """H3: Layer 'L-DRAFT' should become 'L', not 'L-' (removesuffix, not [:-6])."""
        items = [
            LintItem(item_id="a", geometry_type="LINE", layer="L-DRAFT", status=GeometryStatus.ACCEPTED),
        ]
        result = SurveyorLinter.promote_to_certified(items, "P", "1", audit_logger)
        assert result is True
        assert items[0].layer == "L", "expected 'L' not 'L-'"


class TestNovelEdgeCases:
    """Novel edge cases: non-list entities, non-dict entity/flag, NaN confidence, None layer."""

    def test_entities_not_a_list_does_not_crash(self, linter, tmp_output, sample_classification):
        """N1: manifest['entities'] not a list (e.g. None or dict) should not raise; yields 0 items."""
        for bad_entities in (None, {}, "string"):
            manifest = {"entities": bad_entities}
            ctx = PipelineContext(
                input_path="/f.las", output_dir=tmp_output,
                manifest=manifest,
                extraction=ExtractionResult(),
                classification=sample_classification,
            )
            result = linter.run(ctx)
            assert result.success is True
            assert isinstance(result.data["lint_items"], list)
            assert len(result.data["lint_items"]) == 0

    def test_entity_in_entities_not_dict_skipped_or_safe(self, linter, tmp_output, sample_classification):
        """N2: entity that is not a dict (e.g. string) is skipped; no AttributeError."""
        manifest = {
            "entities": [
                {"id": "a", "type": "T", "layer": "L"},
                "not a dict",
                42,
            ],
        }
        ctx = PipelineContext(
            input_path="/f.las", output_dir=tmp_output,
            manifest=manifest,
            extraction=ExtractionResult(),
            classification=sample_classification,
        )
        result = linter.run(ctx)
        assert result.success is True
        assert len(result.data["lint_items"]) == 1
        assert result.data["lint_items"][0].item_id == "a"

    def test_qa_flags_entry_not_dict_does_not_crash(self, linter, tmp_path):
        """N3: qa_flags entry that is not a dict (e.g. string) should not AttributeError."""
        items = [LintItem(item_id="a", geometry_type="LINE", layer="L", confidence=0.5)]
        ext = ExtractionResult(qa_flags=["warning: something", {"severity": "info", "message": "ok"}])
        ws_path = tmp_path / "ws.txt"
        linter._write_review_worksheet(items, ext, ws_path)
        assert ws_path.exists()

    def test_nan_confidence_serializes_in_report(self, linter, sample_classification):
        """N4: item with float('nan') confidence should not break JSON / report."""
        import math
        items = [
            LintItem(item_id="a", geometry_type="LINE", layer="L", confidence=float("nan")),
            LintItem(item_id="b", geometry_type="LINE", layer="L", confidence=0.8),
        ]
        report = linter._generate_lint_report(items, None)
        assert "items" in report
        assert len(report["items"]) == 2
        # Serialize to JSON (same path as run())
        import json
        json_str = json.dumps(report, indent=2, default=str)
        assert "a" in json_str and "b" in json_str

    def test_promote_layer_none_does_not_crash(self, audit_logger):
        """N5: LintItem with layer=None (e.g. mutated) should not AttributeError in promote_to_certified."""
        item = LintItem(item_id="a", geometry_type="LINE", layer="L-DRAFT", status=GeometryStatus.ACCEPTED)
        item.layer = None  # simulate mutation or bad data
        result = SurveyorLinter.promote_to_certified([item], "P", "1", audit_logger)
        assert result is True
        assert item.status == GeometryStatus.CERTIFIED
