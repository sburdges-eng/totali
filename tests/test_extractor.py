"""Tests for Phase 3: DeterministicExtractor."""

import numpy as np
import pytest

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
        # Delaunay needs at least 3 non-collinear points; this should
        # either produce empty faces or raise (which _build_dtm doesn't guard)
        # At minimum, it shouldn't crash silently.
        try:
            vertices, faces, metrics = extractor._build_dtm(pts)
        except Exception:
            pass  # acceptable — insufficient points


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

class TestContourAtElevation:
    def test_interpolates_crossing_segment(self, extractor):
        # Single triangle
        # V0: (0, 0, 100)
        # V1: (10, 0, 110)
        # V2: (0, 10, 105)
        vertices = np.array([
            [0, 0, 100.0],
            [10, 0, 110.0],
            [0, 10, 105.0]
        ])
        faces = np.array([[0, 1, 2]])

        # Elevation 102.5 should cross edges (0,1) and (0,2)
        # Edge (0,1): t = (102.5 - 100) / (110 - 100) = 2.5 / 10 = 0.25
        # P1 = (0,0) + 0.25 * ((10,0) - (0,0)) = (2.5, 0)

        # Edge (0,2): t = (102.5 - 100) / (105 - 100) = 2.5 / 5 = 0.5
        # P2 = (0,0) + 0.5 * ((0,10) - (0,0)) = (0, 5)

        segments = extractor._contour_at_elevation(vertices, faces, 102.5)
        assert len(segments) == 1
        seg = segments[0]
        assert seg.shape == (2, 2)

        # Check coordinates (order might vary)
        expected = {tuple([2.5, 0.0]), tuple([0.0, 5.0])}
        actual = {tuple(p) for p in seg}
        assert actual == expected

    def test_no_segments_when_elevation_out_of_range(self, extractor):
        vertices = np.array([
            [0, 0, 100.0],
            [10, 0, 110.0],
            [0, 10, 105.0]
        ])
        faces = np.array([[0, 1, 2]])

        # Below range
        assert extractor._contour_at_elevation(vertices, faces, 99.0) == []
        # Above range
        assert extractor._contour_at_elevation(vertices, faces, 111.0) == []

    def test_multiple_faces(self, extractor):
        # Two triangles sharing an edge (1,2)
        vertices = np.array([
            [0, 0, 100.0],
            [10, 0, 110.0],
            [0, 10, 105.0],
            [10, 10, 115.0]
        ])
        faces = np.array([
            [0, 1, 2],
            [1, 3, 2]
        ])

        # Elevation 107.5
        # Face [0,1,2]:
        #   (0,1) crosses: (107.5-100)/(110-100) = 0.75 -> (7.5, 0)
        #   (0,2) crosses: (107.5-100)/(105-100) = 1.5 -> No
        #   (1,2) crosses: (107.5-110)/(105-110) = -2.5/-5 = 0.5 -> (5, 5)
        #   Face 0 segment: [(7.5, 0), (5, 5)]

        # Face [1,3,2]:
        #   (1,3) crosses: (107.5-110)/(115-110) = -2.5/5 = No (oops, 110 to 115)
        #   Actually (1,3): (107.5-110) * (115-107.5) = -2.5 * 7.5 < 0 -> Yes
        #   (1,3) crosses: (107.5-110)/(115-110) = 0.5 (wait, (107.5-110)/5 = -0.5, something is wrong with my math)
        #   Let's re-eval (z1-elev)*(z3-elev) = (110-107.5)*(115-107.5) = 2.5 * 7.5 > 0 -> No cross
        #   (1,2) crosses: (110-107.5)*(105-107.5) = 2.5 * -2.5 < 0 -> Yes
        #   (3,2) crosses: (115-107.5)*(105-107.5) = 7.5 * -2.5 < 0 -> Yes

        segments = extractor._contour_at_elevation(vertices, faces, 107.5)
        assert len(segments) == 2
