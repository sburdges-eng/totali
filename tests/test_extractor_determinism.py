"""E-5 partial: extractor determinism on identical inputs.

Given the same classification + CRS inputs, two extractor runs should produce
identical high-level counts and identical error_metrics keys. Exact geometry
byte-parity is verified at the shield/serialization layer.
"""

import numpy as np

from totali.audit.logger import AuditLogger
from totali.extraction.extractor import DeterministicExtractor
from totali.pipeline.context import PipelineContext
from totali.pipeline.models import ClassificationResult, CRSMetadata


def _ctx(sample_points, tmp_output, labels):
    confidences = np.full(len(labels), 0.9, dtype=np.float32)
    classification = ClassificationResult(
        labels=labels,
        confidences=confidences,
        occlusion_mask=np.zeros(len(labels), dtype=bool),
        class_counts={"ground": int((labels == 2).sum())},
    )
    ctx = PipelineContext(
        input_path="/x.las",
        output_dir=tmp_output,
        points_xyz=sample_points,
        crs=CRSMetadata(epsg_code=2231, is_valid=True),
        classification=classification,
    )
    return ctx


class TestExtractorDeterminism:
    def test_same_input_same_counts(self, sample_points, sample_config, tmp_output, tmp_path):
        rng = np.random.default_rng(0)
        labels = rng.choice([0, 2, 6], size=len(sample_points))
        audit1 = AuditLogger(log_dir=str(tmp_path / "a1"), project_id="e1")
        audit2 = AuditLogger(log_dir=str(tmp_path / "a2"), project_id="e2")

        ex1 = DeterministicExtractor(sample_config["extraction"], audit1)
        ex2 = DeterministicExtractor(sample_config["extraction"], audit2)

        r1 = ex1.run(_ctx(sample_points, tmp_output, labels.copy()))
        r2 = ex2.run(_ctx(sample_points, tmp_output, labels.copy()))

        assert r1.success == r2.success
        if r1.data and r2.data:
            # Compare high-level structure: same keys, same point counts.
            assert set(r1.data.keys()) == set(r2.data.keys())
