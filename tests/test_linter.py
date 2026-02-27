"""Tests for totali.linting.surveyor_lint"""
import json
import pytest
from pathlib import Path

from totali.linting.surveyor_lint import SurveyorLinter
from totali.pipeline.models import (
    GeometryStatus, LintItem, OcclusionType, ClassificationResult,
    ExtractionResult,
)


@pytest.fixture
def sample_manifest():
    return {
        "entities": [
            {"id": "aaa", "type": "POLYLINE", "layer": "TOTaLi-SURV-BRKLN-DRAFT", "source_hash": "abc123"},
            {"id": "bbb", "type": "3DFACE", "layer": "TOTaLi-SURV-DTM-DRAFT", "source_hash": "def456"},
            {"id": "ccc", "type": "OCCLUSION_ZONE", "layer": "TOTaLi-QA-OCCLUSION", "source_hash": "ghi789"},
        ]
    }


def test_linter_generates_report(config, audit, output_dir, sample_manifest):
    linter = SurveyorLinter(config["linting"], audit)
    context = {
        "manifest": sample_manifest,
        "extraction": ExtractionResult(),
        "classification": ClassificationResult(mean_confidence=0.8),
        "output_dir": output_dir,
    }
    result = linter.run(context)
    assert result.success is True
    report = result.data["lint_report"]
    assert report["auto_promote"] is False
    assert report["summary"]["total_items"] == 3


def test_linter_all_items_draft(config, audit, output_dir, sample_manifest):
    linter = SurveyorLinter(config["linting"], audit)
    context = {
        "manifest": sample_manifest,
        "extraction": ExtractionResult(),
        "classification": ClassificationResult(mean_confidence=0.8),
        "output_dir": output_dir,
    }
    result = linter.run(context)
    for item in result.data["lint_items"]:
        assert item.status == GeometryStatus.DRAFT


def test_linter_writes_worksheet(config, audit, output_dir, sample_manifest):
    linter = SurveyorLinter(config["linting"], audit)
    context = {
        "manifest": sample_manifest,
        "extraction": ExtractionResult(),
        "classification": ClassificationResult(mean_confidence=0.8),
        "output_dir": output_dir,
    }
    linter.run(context)
    ws = output_dir / "review_worksheet.txt"
    assert ws.exists()
    content = ws.read_text()
    assert "PLS Signature" in content
    assert "DISABLED" in content


def test_accept_reject_workflow(audit):
    item = LintItem(item_id="x1", geometry_type="POLYLINE", layer="TEST-DRAFT")
    assert item.status == GeometryStatus.DRAFT

    SurveyorLinter.accept_item(item, "John Doe PLS", audit, "Looks good")
    assert item.status == GeometryStatus.ACCEPTED
    assert item.reviewer == "John Doe PLS"

    item2 = LintItem(item_id="x2", geometry_type="3DFACE", layer="TEST-DRAFT")
    SurveyorLinter.reject_item(item2, "John Doe PLS", audit, "Bad geometry")
    assert item2.status == GeometryStatus.REJECTED


def test_promote_blocks_on_draft(audit):
    items = [
        LintItem(item_id="a", geometry_type="X", layer="L-DRAFT", status=GeometryStatus.ACCEPTED),
        LintItem(item_id="b", geometry_type="X", layer="L-DRAFT", status=GeometryStatus.DRAFT),
    ]
    ok = SurveyorLinter.promote_to_certified(items, "PLS Name", "CO-12345", audit)
    assert ok is False
    assert items[0].status == GeometryStatus.ACCEPTED  # unchanged


def test_promote_succeeds_all_reviewed(audit):
    items = [
        LintItem(item_id="a", geometry_type="X", layer="TOTaLi-SURV-BRKLN-DRAFT",
                 status=GeometryStatus.ACCEPTED),
        LintItem(item_id="b", geometry_type="X", layer="TOTaLi-SURV-DTM-DRAFT",
                 status=GeometryStatus.REJECTED),
    ]
    ok = SurveyorLinter.promote_to_certified(items, "Jane Smith PLS", "CO-99999", audit)
    assert ok is True
    assert items[0].status == GeometryStatus.CERTIFIED
    assert items[0].layer == "TOTaLi-SURV-BRKLN"  # -DRAFT stripped
    assert items[1].status == GeometryStatus.REJECTED  # unchanged


def test_auto_promote_always_false(config, audit):
    linter = SurveyorLinter(config["linting"], audit)
    assert linter.auto_promote is False
    # Even if config tried to set it
    bad_config = dict(config["linting"])
    bad_config["auto_promote"] = True
    linter2 = SurveyorLinter(bad_config, audit)
    assert linter2.auto_promote is False


def test_occlusion_detected_for_occlusion_layer(config, audit, output_dir):
    linter = SurveyorLinter(config["linting"], audit)
    manifest = {
        "entities": [
            {"id": "occ1", "type": "OCCLUSION_ZONE", "layer": "TOTaLi-QA-OCCLUSION", "source_hash": "x"},
        ]
    }
    er = ExtractionResult()
    er.occlusion_zones = [[[0, 0], [1, 0], [1, 1]]]  # non-empty
    context = {
        "manifest": manifest,
        "extraction": er,
        "classification": ClassificationResult(mean_confidence=0.7),
        "output_dir": output_dir,
    }
    result = linter.run(context)
    occ_items = [i for i in result.data["lint_items"] if i.occlusion != OcclusionType.NONE]
    assert len(occ_items) == 1
