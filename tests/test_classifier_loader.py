"""U1: classifier wired to the deterministic sha256/manifest model loader.

The HITL classifier loads a real ONNX model through the existing
``totali.models.loader.load_onnx`` contract (M-1/M-2) instead of an ad-hoc
``InferenceSession``. Benign absence (no model / no onnxruntime) falls back to
the deterministic rule-based path; a hash or manifest MISMATCH is NOT benign and
must fail loudly rather than silently classify with a tampered/wrong model.

The model-choice itself (adapt pretrained vs train custom, KTD3) is the
data-dependent research spike and is intentionally out of scope — this pins the
loading *contract*, which is fully implementable now.
"""

from __future__ import annotations

import hashlib

import pytest

from totali.models.loader import ModelHashMismatch
from totali.pipeline.models import ClassificationResult
from totali.segmentation.classifier import PointCloudClassifier


def _mk(audit, **cfg):
    base = {
        "device": "cpu",
        "confidence_threshold": 0.75,
        "occlusion_threshold": 0.30,
        "batch_size": 256,
        "voxel_size": 0.05,
        "classes": {0: "unclassified", 2: "ground", 6: "building"},
    }
    base.update(cfg)
    return PointCloudClassifier(base, audit)


class TestBenignFallback:
    def test_missing_model_falls_back(self, audit_logger):
        cls = _mk(audit_logger, model_path="models/does_not_exist.onnx")
        assert cls._load_model() is False
        warnings = [e for e in audit_logger.get_events("classify") if "warning" in e["data"]]
        assert warnings, "missing model must log a fallback warning, not crash"

    def test_present_model_without_runtime_falls_back(self, audit_logger, tmp_path):
        # onnxruntime is stubbed in the test env (no InferenceSession) -> the
        # loader raises ImportError -> classifier falls back gracefully.
        model = tmp_path / "model.onnx"
        model.write_bytes(b"not a real onnx but the hash check passes")
        cls = _mk(audit_logger, model_path=str(model), manifest={})
        assert cls._load_model() is False


class TestLoudFailureOnHashMismatch:
    def test_hash_mismatch_raises_not_silent_fallback(self, audit_logger, tmp_path):
        model = tmp_path / "model.onnx"
        model.write_bytes(b"tampered weights")
        wrong_sha = "0" * 64  # deliberately not the file's hash
        cls = _mk(audit_logger, model_path=str(model), expected_sha256=wrong_sha, manifest={})
        with pytest.raises(ModelHashMismatch):
            cls._load_model()

    def test_matching_hash_does_not_raise_hash_error(self, audit_logger, tmp_path):
        model = tmp_path / "model.onnx"
        payload = b"some bytes"
        model.write_bytes(payload)
        good_sha = hashlib.sha256(payload).hexdigest()
        cls = _mk(audit_logger, model_path=str(model), expected_sha256=good_sha, manifest={})
        # Hash passes; runtime is stubbed -> ImportError -> benign fallback (False),
        # never a ModelHashMismatch.
        assert cls._load_model() is False


class TestAdvisoryFlag:
    def test_run_output_is_non_authoritative(self, audit_logger, sample_points, sample_las, tmp_output):
        from totali.pipeline.context import PipelineContext
        from totali.pipeline.models import CRSMetadata, PointCloudStats

        cls = _mk(audit_logger, model_path="models/absent.onnx")
        ctx = PipelineContext(
            input_path="/f.las", output_dir=tmp_output,
            points_xyz=sample_points, las=sample_las,
            crs=CRSMetadata(epsg_code=2231, is_valid=True),
            stats=PointCloudStats(point_count=len(sample_points)), input_hash="abc",
        )
        result = cls.run(ctx)
        cls_out: ClassificationResult = result.data["classification"]
        assert cls_out.authoritative is False
