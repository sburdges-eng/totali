"""S-3: classes map is config-driven; default empty; keys are integer class IDs."""

import pytest

from totali.audit.logger import AuditLogger
from totali.segmentation.classifier import PointCloudClassifier


def _mk(classes):
    audit = AuditLogger(log_dir="audit_logs", project_id="t")
    config = {
        "model_path": "models/none.onnx",
        "device": "cpu",
        "confidence_threshold": 0.75,
        "occlusion_threshold": 0.30,
        "batch_size": 256,
        "voxel_size": 0.05,
        "classes": classes,
    }
    return PointCloudClassifier(config, audit)


class TestClassesFromConfig:
    def test_class_map_passed_through(self):
        cls = _mk({0: "unclassified", 2: "ground", 6: "building"})
        assert cls.classes == {0: "unclassified", 2: "ground", 6: "building"}

    def test_empty_default(self):
        audit = AuditLogger(log_dir="audit_logs", project_id="empty")
        cls = PointCloudClassifier({}, audit)
        assert cls.classes == {}

    def test_extended_classes_preserved(self):
        cls = _mk({14: "wire_conductor", 64: "curb", 65: "hardscape"})
        assert 14 in cls.classes
        assert cls.classes[64] == "curb"


class TestConfigThresholds:
    @pytest.mark.parametrize("value", [0.5, 0.75, 0.85])
    def test_confidence_threshold_preserved(self, value):
        audit = AuditLogger(log_dir="audit_logs", project_id="th")
        cls = PointCloudClassifier({"confidence_threshold": value}, audit)
        assert cls.confidence_threshold == value

    def test_voxel_and_batch_size_preserved(self):
        audit = AuditLogger(log_dir="audit_logs", project_id="bv")
        cls = PointCloudClassifier(
            {"batch_size": 1024, "voxel_size": 0.1}, audit
        )
        assert cls.batch_size == 1024
        assert cls.voxel_size == 0.1
