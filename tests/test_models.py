"""Tests for totali.pipeline.models"""
import numpy as np
import pytest
from totali.pipeline.models import (
    GeometryStatus, OcclusionType, CRSMetadata, PointCloudStats,
    ClassificationResult, ExtractionResult, HealingReport,
    PhaseResult, PipelineResult, LintItem,
)


def test_geometry_status_values():
    assert GeometryStatus.DRAFT.value == "DRAFT"
    assert GeometryStatus.CERTIFIED.value == "CERTIFIED"


def test_geometry_status_is_string():
    assert isinstance(GeometryStatus.DRAFT, str)
    assert GeometryStatus.DRAFT == "DRAFT"


def test_occlusion_type_values():
    assert OcclusionType.NONE.value == "none"
    assert OcclusionType.CANOPY.value == "canopy"


def test_crs_metadata_defaults():
    crs = CRSMetadata(epsg_code=2232)
    assert crs.horizontal_unit == "US_survey_foot"
    assert crs.is_valid is False
    assert crs.validation_errors == []


def test_classification_result_defaults():
    cr = ClassificationResult()
    assert cr.labels is None
    assert cr.mean_confidence == 0.0
    assert cr.class_counts == {}


def test_extraction_result_defaults():
    er = ExtractionResult()
    assert er.dtm_vertices is None
    assert er.breaklines == []
    assert er.contours_minor == []


def test_healing_report_defaults():
    hr = HealingReport()
    assert hr.input_entity_count == 0
    assert hr.issues == []


def test_phase_result():
    pr = PhaseResult(phase="test", success=True, message="ok")
    assert pr.phase == "test"
    assert pr.data == {}


def test_lint_item_defaults():
    li = LintItem(item_id="abc", geometry_type="POLYLINE", layer="TEST")
    assert li.status == GeometryStatus.DRAFT
    assert li.confidence == 0.0
    assert li.reviewer is None
