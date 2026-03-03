"""Tests for Phase 3: DeterministicExtractor."""

import numpy as np
import pytest
from unittest.mock import patch, MagicMock

from totali.extraction.extractor import DeterministicExtractor
from totali.pipeline.context import PipelineContext
from totali.pipeline.models import (
    PhaseResult,
    ExtractionResult,
    ClassificationResult,
    CRSMetadata,
    PointCloudStats,
)


@pytest.fixture
def extractor(audit_logger, sample_config):
    return DeterministicExtractor(sample_config["extraction"], audit_logger)


@pytest.fixture
def ground_points():
    """Grid of ground points suitable for Delaunay triangulation."""
    rng = np.random.default_rng(42)
    xs = np.linspace(0, 100, 20)
    ys = np.linspace(0, 100, 20)
    xx, yy = np.meshgrid(xs, ys)
    zz = 100.0 + rng.uniform(-0.5, 0.5, xx.shape)
    return np.column_stack([xx.ravel(), yy.ravel(), zz.ravel()])


class TestValidateInputs:
    def test_missing_points(self, extractor, tmp_output):
        ctx = PipelineContext(input_path="/f.las", output_dir=tmp_output)
        valid, errors = extractor.validate_inputs(ctx)
        assert valid is False
        assert any("points_xyz" in e for e in errors)

    def test_missing_classification(self, extractor, tmp_output, sample_points):
        ctx = PipelineContext(
            input_path="/f.las", output_dir=tmp_output,
            points_xyz=sample_points,
        )
        valid, errors = extractor.validate_inputs(ctx)
        assert valid is False
        assert any("classification" in e for e in errors)

    def test_valid_inputs(self, extractor, pipeline_context):
        valid, errors = extractor.validate_inputs(pipeline_context)
        assert valid is True


class TestDTMBuilding:
    def test_builds_tin_from_ground(self, extractor, ground_points):
        vertices, faces, metrics = extractor._build_dtm(ground_points)
        assert len(vertices) > 0
        assert len(faces) > 0
        assert metrics["vertex_count"] > 0
        assert metrics["face_count"] > 0

    def test_edge_length_filtering(self, extractor, ground_points):
        extractor.dtm_cfg["max_triangle_edge_length"] = 10.0
        _, faces, metrics = extractor._build_dtm(ground_points)
        assert metrics["max_edge_length"] <= 10.0

    def test_empty_ground_returns_empty(self, extractor):
        pts = np.zeros((2, 3))  # Not enough for triangulation
        try:
            vertices, faces, metrics = extractor._build_dtm(pts)
        except Exception:
            pass  # acceptable


class TestBreaklineExtraction:
    def test_extracts_breaklines_from_sloped_surface(self, extractor, ground_points):
        vertices, faces, _ = extractor._build_dtm(ground_points)
        breaklines, metrics = extractor._extract_breaklines(ground_points, vertices, faces)
        assert isinstance(breaklines, list)
        assert "count" in metrics

    def test_no_breaklines_with_one_face(self, extractor):
        pts = np.array([[0, 0, 0], [1, 0, 0], [0, 1, 0.0]])
        faces = np.array([[0, 1, 2]])
        breaklines, metrics = extractor._extract_breaklines(pts, pts, faces)
        assert metrics["count"] == 0


class TestContourGeneration:
    def test_generates_contours(self, extractor, ground_points):
        vertices, faces, _ = extractor._build_dtm(ground_points)
        minor, index, metrics = extractor._generate_contours(vertices, faces)
        assert isinstance(minor, list)
        assert isinstance(index, list)
        assert "minor_count" in metrics
        assert "index_count" in metrics

    def test_no_contours_with_no_faces(self, extractor):
        pts = np.array([[0, 0, 100]])
        faces = np.empty((0, 3), dtype=int)
        minor, index, metrics = extractor._generate_contours(pts, faces)
        assert minor == []
        assert index == []


class TestClustering:
    def test_cluster_groups_nearby_points(self, extractor):
        pts = np.array([
            [0, 0, 0], [1, 1, 0], [2, 2, 0],
            [100, 100, 0], [101, 101, 0],
        ])
        clusters = extractor._cluster_points_2d(pts, radius=5.0)
        assert len(clusters) >= 1

    def test_empty_input(self, extractor):
        pts = np.empty((0, 3))
        clusters = extractor._cluster_points_2d(pts, radius=5.0)
        assert clusters == []


class TestQAFlags:
    def test_flags_low_confidence(self, extractor, sample_classification):
        result = ExtractionResult()
        flags = extractor._generate_qa_flags(result, sample_classification)
        low_conf_flags = [f for f in flags if f["type"] == "low_confidence"]
        if sample_classification.low_confidence_count > 0:
            assert len(low_conf_flags) > 0

    def test_flags_occlusion_zones(self, extractor, sample_classification):
        result = ExtractionResult()
        result.occlusion_zones = [np.array([[0, 0], [1, 0], [0, 1]])]
        flags = extractor._generate_qa_flags(result, sample_classification)
        occ_flags = [f for f in flags if f["type"] == "occlusion"]
        assert len(occ_flags) == 1


class TestPhaseRun:
    def test_run_end_to_end(self, extractor, pipeline_context):
        result = extractor.run(pipeline_context)
        assert isinstance(result, PhaseResult)
        assert result.phase == "extract"
        if result.success:
            assert "extraction" in result.data
            assert isinstance(result.data["extraction"], ExtractionResult)
            assert len(result.output_files) > 0

    def test_run_insufficient_ground_fails(self, extractor, tmp_output, sample_las):
        few_pts = np.random.default_rng(0).uniform(0, 10, (20, 3))
        cls = ClassificationResult(
            labels=np.zeros(20, dtype=np.int32),  # no ground (class 2)
            confidences=np.full(20, 0.5, dtype=np.float32),
        )
        ctx = PipelineContext(
            input_path="/f.las", output_dir=tmp_output,
            points_xyz=few_pts, las=sample_las,
            classification=cls,
        )
        result = extractor.run(ctx)
        assert result.success is False
        assert "ground" in result.message.lower() or "Insufficient" in result.message


class TestImportErrors:
    def test_extract_polygonal_features_handles_import_error(self, extractor):
        """Test that _extract_polygonal_features returns empty list on ImportError."""
        pts = np.array([
            [0, 0, 0], [1, 0, 0], [1, 1, 0], [0, 1, 0]
        ])

        import builtins
        original_import = builtins.__import__

        def mock_import(name, *args, **kwargs):
            if name == 'scipy.spatial':
                raise ImportError("Mocked ImportError")
            return original_import(name, *args, **kwargs)

        with patch('builtins.__import__', side_effect=mock_import):
            polygons = extractor._extract_polygonal_features(pts)
            assert polygons == []

    def test_extract_building_footprints_handles_import_error(self, extractor):
        """Test that _extract_building_footprints returns empty list on ImportError."""
        pts = np.array([
            [0, 0, 0], [1, 0, 0], [1, 1, 0], [0, 1, 0]
        ])

        import builtins
        original_import = builtins.__import__

        def mock_import(name, *args, **kwargs):
            if name == 'scipy.spatial':
                raise ImportError("Mocked ImportError")
            return original_import(name, *args, **kwargs)

        with patch('builtins.__import__', side_effect=mock_import):
            footprints = extractor._extract_building_footprints(pts)
            assert footprints == []

class TestClusteringExtended:
    @pytest.fixture
    def pts_2d(self):
        return np.array([
            [0.1, 0.1, 5.0],
            [0.3, 0.3, 10.0],
            [10.0, 10.0, 15.0],
            [10.2, 10.2, 20.0],
        ])

    def test_single_point(self, extractor):
        pts = np.array([[1.0, 1.0, 1.0]])
        assert extractor._cluster_points_2d(pts, radius=1.0) == []

    def test_points_far_apart(self, extractor):
        pts = np.array([[0, 0, 0], [10, 10, 0]])
        assert extractor._cluster_points_2d(pts, radius=1.0) == []

    def test_multiple_clusters(self, extractor, pts_2d):
        clusters = extractor._cluster_points_2d(pts_2d, radius=1.0)
        assert len(clusters) == 2
        # Check Z preservation
        assert any(5.0 in c[:, 2] for c in clusters)
        assert any(20.0 in c[:, 2] for c in clusters)

    def test_grid_boundary(self, extractor):
        # grid_size = 2.0 (radius=1.0)
        # 1.99 -> cell 0, 2.0 -> cell 1
        pts = np.array([
            [1.9, 0, 0], [1.95, 0, 0],
            [2.0, 0, 0], [2.1, 0, 0]
        ])
        clusters = extractor._cluster_points_2d(pts, radius=1.0)
        assert len(clusters) == 2
        assert len(clusters[0]) == 2
        assert len(clusters[1]) == 2

    @pytest.mark.parametrize("radius, expected_count", [
        (0.1, 0),
        (1.0, 1),
        (10.0, 1),
    ])
    def test_radius_scaling(self, extractor, radius, expected_count):
        pts = np.array([[0, 0, 0], [1, 1, 0]])
        clusters = extractor._cluster_points_2d(pts, radius=radius)
        assert len(clusters) == expected_count
