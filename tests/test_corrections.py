"""U1: surveyor correction capture (HITL training-data flywheel).

A `CorrectionStore` records surveyor corrections of the classifier's *advisory*
labels as JSONL deltas keyed to the audit record ID. It is a pure sink: it
accumulates labeled training data and NEVER mutates authoritative geometry or
the classifier output. The model-selection / training half of U1 (adapt vs
train) is the data-dependent research spike and is intentionally out of scope
here — this exercises the capture contract, which is fully implementable now.
"""

from __future__ import annotations

import json

import numpy as np

from totali.pipeline.models import ClassificationResult
from totali.segmentation.corrections import Correction, CorrectionStore


class TestRecordCorrection:
    def test_record_writes_labeled_record_linked_to_audit_id(self, tmp_path):
        store = CorrectionStore(tmp_path)
        c = store.record(
            audit_id="run-abc123",
            point_index=42,
            original_label=5,
            corrected_label=2,
            surveyor="alice",
            notes="misclassified ground as high veg",
        )
        assert isinstance(c, Correction)
        assert c.audit_id == "run-abc123"
        assert c.original_label == 5
        assert c.corrected_label == 2
        assert c.surveyor == "alice"
        assert c.timestamp  # populated

        # Persisted as one JSONL line keyed to the audit id.
        lines = (tmp_path / "corrections.jsonl").read_text().splitlines()
        assert len(lines) == 1
        rec = json.loads(lines[0])
        assert rec["audit_id"] == "run-abc123"
        assert rec["point_index"] == 42
        assert rec["corrected_label"] == 2

    def test_records_accumulate_as_training_data(self, tmp_path):
        store = CorrectionStore(tmp_path)
        store.record("run-1", 1, 5, 2, "alice")
        store.record("run-1", 2, 4, 6, "alice")
        store.record("run-2", 3, 0, 2, "bob")
        assert len(store.all_corrections()) == 3
        assert len(store.corrections_for("run-1")) == 2
        assert len(store.corrections_for("run-2")) == 1

    def test_export_training_data_shape(self, tmp_path):
        store = CorrectionStore(tmp_path)
        store.record("run-1", 7, 5, 2, "alice")
        data = store.export_training_data()
        assert data == [
            {
                "audit_id": "run-1",
                "point_index": 7,
                "label": 2,
                "original_label": 5,
                "surveyor": "alice",
            }
        ]

    def test_persists_across_store_instances(self, tmp_path):
        CorrectionStore(tmp_path).record("run-1", 1, 5, 2, "alice")
        # A fresh store over the same dir reads back the accumulated flywheel.
        assert len(CorrectionStore(tmp_path).all_corrections() ) == 1


class TestGeometryUntouched:
    def test_recording_does_not_mutate_classifier_output(self, tmp_path):
        labels = np.array([5, 5, 5, 5], dtype=np.int32)
        confidences = np.array([0.4, 0.4, 0.4, 0.4], dtype=np.float32)
        result = ClassificationResult(labels=labels.copy(), confidences=confidences.copy())

        store = CorrectionStore(tmp_path)
        store.record("run-1", 0, original_label=int(result.labels[0]), corrected_label=2, surveyor="alice")

        # The advisory classifier output is authoritative-geometry-adjacent and
        # must be left byte-identical: corrections live in their own sink.
        assert np.array_equal(result.labels, labels)
        assert result.authoritative is False


class TestRecordBatch:
    def test_record_batch_writes_all(self, tmp_path):
        store = CorrectionStore(tmp_path)
        out = store.record_batch(
            audit_id="run-1",
            point_indices=[1, 2, 3],
            original_labels=[5, 4, 0],
            corrected_labels=[2, 6, 2],
            surveyor="alice",
        )
        assert len(out) == 3
        assert len(store.corrections_for("run-1")) == 3
