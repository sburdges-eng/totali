"""S-7: ClassificationResult.authoritative is hardcoded False.

Invariant: ML segmentation output is never treated as authoritative. The flag
exists so downstream consumers can guard against accidental promotion; the
constructor refuses any attempt to set True.
"""

import numpy as np
import pytest

from totali.pipeline.models import ClassificationResult
from totali.segmentation.classifier import PointCloudClassifier


class TestAuthoritativeFlag:
    def test_default_false(self):
        c = ClassificationResult()
        assert c.authoritative is False

    def test_construction_with_data_still_false(self):
        c = ClassificationResult(
            labels=np.array([0, 2, 6], dtype=np.int32),
            confidences=np.array([0.5, 0.9, 0.85], dtype=np.float32),
        )
        assert c.authoritative is False

    def test_explicit_true_rejected(self):
        with pytest.raises(ValueError) as exc:
            ClassificationResult(authoritative=True)
        assert "authoritative" in str(exc.value).lower()

    @pytest.mark.parametrize("truthy", [1, "true", "yes", [1]])
    def test_truthy_values_rejected(self, truthy):
        with pytest.raises(ValueError):
            ClassificationResult(authoritative=truthy)


class TestClassifierEmitsNonAuthoritative:
    def test_rule_based_classifier_output_is_non_authoritative(
        self, audit_logger, sample_config
    ):
        """The fallback rule-based path must produce non-authoritative output."""
        cls = PointCloudClassifier(sample_config["segmentation"], audit_logger)
        rng = np.random.default_rng(0)
        xyz = rng.uniform(0, 100, (200, 3))
        result = cls._classify_rules(xyz, las=None)
        assert result.authoritative is False
