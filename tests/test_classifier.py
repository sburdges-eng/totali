"""Tests for Phase 2: PointCloudClassifier."""

import numpy as np
import pytest

from totali.segmentation.classifier import PointCloudClassifier
from totali.pipeline.context import PipelineContext
from totali.pipeline.models import PhaseResult, ClassificationResult, CRSMetadata, PointCloudStats


@pytest.fixture
def classifier(audit_logger, sample_config):
    return PointCloudClassifier(sample_config["segmentation"], audit_logger)


class TestValidateInputs:
    def test_missing_points(self, classifier, tmp_output):
        ctx = PipelineContext(input_path="/f.las", output_dir=tmp_output)
        valid, errors = classifier.validate_inputs(ctx)
        assert valid is False
        assert any("points_xyz" in e for e in errors)

    def test_missing_las(self, classifier, tmp_output):
        ctx = PipelineContext(
            input_path="/f.las", output_dir=tmp_output,
            points_xyz=np.zeros((10, 3)),
        )
        valid, errors = classifier.validate_inputs(ctx)
        assert valid is False
        assert any("las" in e for e in errors)

    def test_valid_inputs(self, classifier, tmp_output, sample_points, sample_las):
        ctx = PipelineContext(
            input_path="/f.las", output_dir=tmp_output,
            points_xyz=sample_points, las=sample_las,
        )
        valid, errors = classifier.validate_inputs(ctx)
        assert valid is True


class TestRuleBasedFallback:
    def test_classify_rules_returns_valid_result(self, classifier, sample_points, sample_las):
        result = classifier._classify_rules(sample_points, sample_las)
        assert isinstance(result, ClassificationResult)
        assert result.labels is not None
        assert len(result.labels) == len(sample_points)
        assert result.confidences is not None
        assert len(result.confidences) == len(sample_points)

    def test_classify_rules_ground_band(self, classifier, sample_las):
        """Lowest 15% of elevation should be classified as ground (class 2)."""
        n = 100
        xyz = np.column_stack([
            np.zeros(n), np.zeros(n),
            np.linspace(0, 100, n),
        ])
        sample_las.classification = np.zeros(n, dtype=np.int32)
        result = classifier._classify_rules(xyz, sample_las)
        ground_mask = result.labels == 2
        assert ground_mask.sum() > 0

    def test_existing_classification_trusted(self, classifier):
        """When LAS already has classification, it should be preferred."""
        import laspy
        las = laspy.read("fake")
        n = len(las.points)
        las.classification = np.full(n, 6, dtype=np.uint8)  # all building
        xyz = np.column_stack([las.x, las.y, las.z])

        result = classifier._classify_rules(xyz, las)
        building_count = np.sum(result.labels == 6)
        assert building_count == n
        assert np.all(result.confidences[result.labels == 6] == 0.85)


class TestOcclusionDetection:
    def test_low_confidence_flagged_as_occlusion(self, classifier, sample_points):
        result = ClassificationResult(
            labels=np.full(len(sample_points), 2, dtype=np.int32),
            confidences=np.full(len(sample_points), 0.1, dtype=np.float32),
        )
        mask = classifier._detect_occlusions(sample_points, result)
        assert mask.any(), "Very low confidence points should be flagged"

    def test_high_confidence_not_flagged(self, classifier, sample_points):
        result = ClassificationResult(
            labels=np.full(len(sample_points), 2, dtype=np.int32),
            confidences=np.full(len(sample_points), 0.95, dtype=np.float32),
        )
        mask = classifier._detect_occlusions(sample_points, result)
        assert not mask.all(), "High confidence points shouldn't all be flagged"

    def test_under_canopy_occlusion(self, classifier):
        """Points between ground and high vegetation should be flagged."""
        # ground=2, high_veg=5
        xyz = np.array([
            [0, 0, 0],   # Ground
            [0, 0, 5],   # Under canopy point
            [0, 0, 10],  # High vegetation
        ], dtype=np.float32)

        result = ClassificationResult(
            labels=np.array([2, 0, 5], dtype=np.int32),
            confidences=np.array([0.9, 0.9, 0.9], dtype=np.float32),
        )

        mask = classifier._detect_occlusions(xyz, result)

        assert not mask[0], "Ground should not be occluded by canopy logic"
        assert mask[1], "Point between ground and canopy should be occluded"
        assert not mask[2], "Canopy itself should not be occluded by canopy logic"

    def test_no_occlusion_without_vegetation(self, classifier):
        """Canopy logic should not trigger if no high vegetation is present."""
        xyz = np.array([
            [0, 0, 0],
            [0, 0, 5],
        ], dtype=np.float32)
        result = ClassificationResult(
            labels=np.array([2, 0], dtype=np.int32),
            confidences=np.array([0.9, 0.9], dtype=np.float32),
        )
        mask = classifier._detect_occlusions(xyz, result)
        assert not mask.any()

    def test_no_occlusion_without_ground(self, classifier):
        """Canopy logic should not trigger if no ground is present."""
        xyz = np.array([
            [0, 0, 5],
            [0, 0, 10],
        ], dtype=np.float32)
        result = ClassificationResult(
            labels=np.array([0, 5], dtype=np.int32),
            confidences=np.array([0.9, 0.9], dtype=np.float32),
        )
        mask = classifier._detect_occlusions(xyz, result)
        assert not mask.any()


class TestFeatureBuilding:
    def test_xyz_only_features(self, classifier):
        import laspy
        las = laspy.read("fake")
        xyz = np.column_stack([las.x, las.y, las.z])
        features = classifier._build_features(xyz, las)
        assert features.shape[0] == len(xyz)
        assert features.shape[1] >= 3


class TestPhaseRun:
    def test_run_produces_classification(self, classifier, tmp_output, sample_points, sample_las):
        ctx = PipelineContext(
            input_path="/f.las", output_dir=tmp_output,
            points_xyz=sample_points, las=sample_las,
            crs=CRSMetadata(epsg_code=2231, is_valid=True),
            stats=PointCloudStats(point_count=len(sample_points)),
            input_hash="abc",
        )
        result = classifier.run(ctx)
        assert isinstance(result, PhaseResult)
        assert result.success is True
        assert result.phase == "segment"
        assert "classification" in result.data
        assert isinstance(result.data["classification"], ClassificationResult)

    def test_run_without_points_fails(self, classifier, tmp_output):
        ctx = PipelineContext(input_path="/f.las", output_dir=tmp_output)
        result = classifier.run(ctx)
        assert result.success is False

    def test_classification_has_stats(self, classifier, tmp_output, sample_points, sample_las):
        ctx = PipelineContext(
            input_path="/f.las", output_dir=tmp_output,
            points_xyz=sample_points, las=sample_las,
            crs=CRSMetadata(epsg_code=2231, is_valid=True),
            stats=PointCloudStats(point_count=len(sample_points)),
            input_hash="abc",
        )
        result = classifier.run(ctx)
        cls = result.data["classification"]
        assert cls.mean_confidence > 0
        assert isinstance(cls.class_counts, dict)
        assert cls.occlusion_mask is not None
