"""S-2 partial: device requested via config, ONNX-missing fallback path.

Current behavior: when onnxruntime is unavailable (as in the CI image — see
conftest stubs), `_load_model()` returns False and the classifier uses
`_classify_rules` as a graceful fallback, emitting an audit warning.

The AGENTIC.md S-2 target is stricter ("fail, do not silently fall back" for
device mismatch). This test documents the current graceful-fallback behavior
so any future hardening is an explicit change with matching test updates.
"""

import numpy as np

from totali.audit.logger import AuditLogger
from totali.segmentation.classifier import PointCloudClassifier


def _mk(device="cpu"):
    audit = AuditLogger(log_dir="audit_logs", project_id=f"dev-{device}")
    config = {
        "model_path": "models/absent.onnx",
        "device": device,
        "confidence_threshold": 0.75,
        "occlusion_threshold": 0.30,
        "batch_size": 256,
        "voxel_size": 0.05,
        "classes": {0: "unclassified", 2: "ground"},
    }
    return PointCloudClassifier(config, audit), audit


class TestDevicePreserved:
    def test_cpu_device_preserved(self):
        cls, _ = _mk("cpu")
        assert cls.device == "cpu"

    def test_cuda_device_preserved(self):
        cls, _ = _mk("cuda")
        assert cls.device == "cuda"

    def test_tensorrt_device_preserved(self):
        cls, _ = _mk("tensorrt")
        assert cls.device == "tensorrt"


class TestFallbackOnMissingModel:
    def test_missing_model_triggers_rule_fallback(self):
        cls, audit = _mk()
        loaded = cls._load_model()
        assert loaded is False
        warnings = [
            e for e in audit.get_events("classify")
            if "warning" in e.get("data", {})
        ]
        assert warnings, "missing-model fallback must log a classify warning"

    def test_rule_classifier_produces_non_authoritative_output(self):
        cls, _ = _mk()
        rng = np.random.default_rng(0)
        xyz = rng.uniform(0, 100, (100, 3))
        result = cls._classify_rules(xyz, las=None)
        assert result.authoritative is False
        assert result.labels is not None
        assert len(result.labels) == 100


class TestConfidenceThresholdWiring:
    def test_low_confidence_threshold_preserved(self):
        audit = AuditLogger(log_dir="audit_logs", project_id="low")
        cls = PointCloudClassifier({"confidence_threshold": 0.1}, audit)
        assert cls.confidence_threshold == 0.1

    def test_high_confidence_threshold_preserved(self):
        audit = AuditLogger(log_dir="audit_logs", project_id="high")
        cls = PointCloudClassifier({"confidence_threshold": 0.99}, audit)
        assert cls.confidence_threshold == 0.99
