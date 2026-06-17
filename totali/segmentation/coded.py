"""Deterministic classification of *coded* survey points (no ML).

Coded survey exports already carry the surveyor's field code per point, so their
class is **deterministic and authoritative** — the field code maps through the
taxonomy to a firm layer, and the crosswalk to a conforming TOTaLi DRAFT layer.
This is the counterpart to the advisory ML :class:`PointCloudClassifier`, which
exists for *raw* (uncoded) LiDAR: here the human already coded the shot, so the
result is authoritative (confidence 1.0), not advisory.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Optional

from totali.cad_shielding.layer_crosswalk import draft_layer_for
from totali.segmentation.dataset import LabeledPoint


@dataclass(frozen=True)
class CodedClassification:
    point_id: str
    field_code: str
    firm_layer: Optional[str]  # taxonomy layer; None when the code is unknown
    draft_layer: str           # conforming TOTaLi DRAFT layer (or QA uncoded)
    authoritative: bool = True  # surveyor-coded -> deterministic, not advisory


def classify_coded_points(
    points: Iterable[LabeledPoint],
    discipline: str = "SURV",
) -> list[CodedClassification]:
    """Assign each coded point its firm + DRAFT layer (deterministic)."""
    return [
        CodedClassification(
            point_id=p.point_id,
            field_code=p.field_code,
            firm_layer=p.layer,
            draft_layer=draft_layer_for(p.layer, discipline),
        )
        for p in points
    ]


def draft_layer_counts(classifications: Iterable[CodedClassification]) -> dict[str, int]:
    """Per-DRAFT-layer point counts (for manifests / QA summaries)."""
    counts: dict[str, int] = {}
    for c in classifications:
        counts[c.draft_layer] = counts.get(c.draft_layer, 0) + 1
    return counts
