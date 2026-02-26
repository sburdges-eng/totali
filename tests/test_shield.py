"""Tests for Phase 4: CADShield."""

import json
from pathlib import Path
from unittest.mock import patch

import numpy as np
import pytest

from totali.cad_shielding.shield import CADShield
from totali.pipeline.context import PipelineContext
from totali.pipeline.models import (
    PhaseResult,
    ExtractionResult,
    HealingReport,
    GeometryStatus,
    CRSMetadata,
    PointCloudStats,
    ClassificationResult,
)


@pytest.fixture
def shield(audit_logger, sample_config):
    return CADShield(sample_config["cad_shielding"], audit_logger)


@pytest.fixture
def sample_extraction():
    """Minimal ExtractionResult with geometry for shield testing."""
    rng = np.random.default_rng(42)
    n_verts = 50
    verts = np.column_stack([
        rng.uniform(0, 100, n_verts),
        rng.uniform(0, 100, n_verts),
        rng.uniform(95, 105, n_verts),
    ])
    faces = np.array([[0, 1, 2], [2, 3, 4], [4, 5, 6]])

    return ExtractionResult(
        dtm_vertices=verts,
        dtm_faces=faces,
        breaklines=[np.array([[0, 0, 100], [10, 10, 101]])],
        contours_minor=[np.array([[0, 0], [5, 5]])],
        contours_index=[np.array([[0, 0], [10, 10]])],
        building_footprints=[np.array([[0, 0], [10, 0], [10, 10], [0, 10]])],
        curb_lines=[],
        wire_lines=[],
        hardscape_polygons=[],
        occlusion_zones=[np.array([[50, 50], [60, 50], [55, 60]])],
    )


class TestValidateInputs:
    def test_missing_extraction(self, shield, tmp_output):
        ctx = PipelineContext(input_path="/f.las", output_dir=tmp_output)
        valid, errors = shield.validate_inputs(ctx)
        assert valid is False
        assert any("extraction" in e for e in errors)

    def test_valid_with_extraction(self, shield, tmp_output, sample_extraction):
        ctx = PipelineContext(
            input_path="/f.las", output_dir=tmp_output,
            extraction=sample_extraction,
        )
        valid, errors = shield.validate_inputs(ctx)
        assert valid is True


class TestGeometryHealing:
    def test_counts_entities(self, shield, sample_extraction):
        report = shield._heal_geometry(sample_extraction)
        assert isinstance(report, HealingReport)
        assert report.input_entity_count > 0

    def test_quarantines_degenerate_faces(self, shield):
        verts = np.array([
            [0, 0, 0], [0, 0, 0], [0, 0, 0],  # degenerate
        ])
        ext = ExtractionResult(dtm_vertices=verts, dtm_faces=np.array([[0, 1, 2]]))
        report = shield._heal_geometry(ext)
        assert report.quarantined_count >= 1

    def test_heals_duplicate_vertices_in_polylines(self, shield):
        line = np.array([[0, 0, 0], [0, 0, 0], [1, 1, 1]])  # dup at start
        ext = ExtractionResult(breaklines=[line])
        report = shield._heal_geometry(ext)
        assert report.healed_count >= 1

    def test_quarantines_short_polyline(self, shield):
        line = np.array([[0, 0, 0]])  # only 1 vertex
        ext = ExtractionResult(breaklines=[line])
        report = shield._heal_geometry(ext)
        assert report.quarantined_count >= 1

    def test_quarantines_polygon_with_too_few_vertices(self, shield):
        poly = np.array([[0, 0], [1, 1]])  # only 2 vertices
        ext = ExtractionResult(building_footprints=[poly])
        report = shield._heal_geometry(ext)
        assert report.quarantined_count >= 1


class TestEntityID:
    def test_unique_ids(self, shield):
        ids = {shield._entity_id() for _ in range(100)}
        assert len(ids) == 100

    def test_id_length(self, shield):
        eid = shield._entity_id()
        assert len(eid) == 12


class TestEntityRecord:
    def test_record_structure(self, shield):
        geo = np.array([[0, 0, 0], [1, 1, 1]])
        rec = shield._entity_record("abc123", "POLYLINE", "LAYER-DRAFT", geo)
        assert rec["id"] == "abc123"
        assert rec["type"] == "POLYLINE"
        assert rec["layer"] == "LAYER-DRAFT"
        assert rec["status"] == GeometryStatus.DRAFT.value
        assert len(rec["source_hash"]) == 16

    def test_source_hash_deterministic(self, shield):
        geo = np.array([[1.0, 2.0, 3.0]])
        r1 = shield._entity_record("a", "LINE", "L", geo)
        r2 = shield._entity_record("b", "LINE", "L", geo)
        assert r1["source_hash"] == r2["source_hash"]

    def test_bbox_added_for_numpy_geometry(self, shield):
        geo = np.array([[1.0, 2.0, 3.0], [4.0, 8.0, 9.0], [2.0, 3.0, 1.0]])
        rec = shield._entity_record("abc123", "POLYLINE", "LAYER-DRAFT", geo)
        assert rec["bbox"] == [1.0, 2.0, 4.0, 8.0]


class TestDXFWriting:
    def test_manual_fallback_writes_file(self, shield, sample_extraction, tmp_path):
        out_path = tmp_path / "test.dxf"
        manifest = shield._write_dxf_manual(sample_extraction, out_path)
        assert out_path.exists()
        assert manifest["format"] == "dxf"
        assert manifest["entity_count"] >= 0
        content = out_path.read_text()
        assert "EOF" in content


class TestPhaseRun:
    @patch("totali.cad_shielding.shield.CADShield._write_dxf")
    def test_run_produces_manifest(self, mock_write, shield, tmp_output, sample_extraction, sample_classification):
        mock_write.side_effect = lambda ext, path: shield._write_dxf_manual(ext, path)
        ctx = PipelineContext(
            input_path="/f.las", output_dir=tmp_output,
            extraction=sample_extraction,
            crs=CRSMetadata(epsg_code=2231, is_valid=True),
            stats=PointCloudStats(point_count=500),
            classification=sample_classification,
            input_hash="test123",
        )
        result = shield.run(ctx)
        assert isinstance(result, PhaseResult)
        assert result.phase == "shield"
        assert result.success is True
        assert "manifest" in result.data
        assert "healing" in result.data
        assert "dxf_path" in result.data

    @patch("totali.cad_shielding.shield.CADShield._write_dxf")
    def test_run_writes_output_files(self, mock_write, shield, tmp_output, sample_extraction, sample_classification):
        mock_write.side_effect = lambda ext, path: shield._write_dxf_manual(ext, path)
        ctx = PipelineContext(
            input_path="/f.las", output_dir=tmp_output,
            extraction=sample_extraction,
            crs=CRSMetadata(epsg_code=2231, is_valid=True),
            stats=PointCloudStats(point_count=500),
            classification=sample_classification,
            input_hash="x",
        )
        result = shield.run(ctx)
        assert len(result.output_files) >= 1
        manifest_file = tmp_output / "entity_manifest.json"
        assert manifest_file.exists()
        data = json.loads(manifest_file.read_text())
        assert "entities" in data

    def test_run_without_extraction_fails(self, shield, tmp_output):
        ctx = PipelineContext(input_path="/f.las", output_dir=tmp_output)
        result = shield.run(ctx)
        assert result.success is False

    @patch("totali.cad_shielding.shield.CADShield._write_dxf")
    def test_all_entities_are_draft(self, mock_write, shield, tmp_output, sample_extraction, sample_classification):
        mock_write.side_effect = lambda ext, path: shield._write_dxf_manual(ext, path)
        ctx = PipelineContext(
            input_path="/f.las", output_dir=tmp_output,
            extraction=sample_extraction,
            crs=CRSMetadata(epsg_code=2231, is_valid=True),
            stats=PointCloudStats(point_count=500),
            classification=sample_classification,
            input_hash="x",
        )
        result = shield.run(ctx)
        manifest = result.data["manifest"]
        for entity in manifest.get("entities", []):
            assert entity["status"] == "DRAFT"
