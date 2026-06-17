"""U1: surveyor correction capture — the HITL training-data flywheel.

The classifier (``totali/segmentation/classifier.py``) produces *advisory,
non-authoritative* labels. When a surveyor corrects one during review, that
correction is captured here as a labeled delta keyed to the audit record ID,
accumulating training data for the next model iteration.

Hard invariants:

- **Never mutates authoritative geometry or classifier output.** This is a
  one-way sink: corrections live in their own JSONL store, separate from the
  DRAFT/certified geometry pipeline.
- **Linked to the audit chain.** Every record carries the ``audit_id`` of the
  run it corrects, so the flywheel is traceable to a specific, hashed pipeline
  execution.

NOTE (KTD3 / scaffold): how this accumulated data is *consumed* — adapt a
pretrained point-cloud model vs. train a custom one — is the data-dependent
research spike that requires the partner dataset. That decision is deliberately
left as a TODO in ``classifier.py``; the capture contract below is complete.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Sequence

_STORE_FILENAME = "corrections.jsonl"


@dataclass(frozen=True)
class Correction:
    """One surveyor correction of an advisory classifier label."""

    audit_id: str
    point_index: int
    original_label: int
    corrected_label: int
    surveyor: str
    timestamp: str
    notes: str = ""


class CorrectionStore:
    """Append-only JSONL store of surveyor corrections under ``store_dir``."""

    def __init__(self, store_dir: str | Path):
        self.store_dir = Path(store_dir)
        self.store_dir.mkdir(parents=True, exist_ok=True)
        self.path = self.store_dir / _STORE_FILENAME

    def record(
        self,
        audit_id: str,
        point_index: int,
        original_label: int,
        corrected_label: int,
        surveyor: str,
        notes: str = "",
        timestamp: Optional[str] = None,
    ) -> Correction:
        """Append one labeled correction. Returns the persisted record.

        Does not touch any geometry or classifier output — only this store.
        """
        correction = Correction(
            audit_id=audit_id,
            point_index=int(point_index),
            original_label=int(original_label),
            corrected_label=int(corrected_label),
            surveyor=surveyor,
            timestamp=timestamp or datetime.now(timezone.utc).isoformat(),
            notes=notes,
        )
        with self.path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(asdict(correction)) + "\n")
        return correction

    def record_batch(
        self,
        audit_id: str,
        point_indices: Sequence[int],
        original_labels: Sequence[int],
        corrected_labels: Sequence[int],
        surveyor: str,
        notes: str = "",
    ) -> list[Correction]:
        """Record many corrections from the same review session."""
        out: list[Correction] = []
        for idx, orig, corr in zip(point_indices, original_labels, corrected_labels):
            out.append(self.record(audit_id, idx, orig, corr, surveyor, notes))
        return out

    def all_corrections(self) -> list[Correction]:
        """Read back every accumulated correction (the training flywheel)."""
        if not self.path.exists():
            return []
        out: list[Correction] = []
        with self.path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    out.append(Correction(**json.loads(line)))
        return out

    def corrections_for(self, audit_id: str) -> list[Correction]:
        """All corrections linked to a specific audit/run id."""
        return [c for c in self.all_corrections() if c.audit_id == audit_id]

    def export_training_data(self) -> list[dict]:
        """Project corrections into a minimal labeled-sample shape for training.

        ``corrected_label`` becomes the supervised ``label``; ``original_label``
        is retained for error analysis. The concrete training pipeline is the
        deferred KTD3 spike (needs the partner dataset).
        """
        return [
            {
                "audit_id": c.audit_id,
                "point_index": c.point_index,
                "label": c.corrected_label,
                "original_label": c.original_label,
                "surveyor": c.surveyor,
            }
            for c in self.all_corrections()
        ]
