"""U1 flywheel seed: build a labeled point dataset from coded survey exports.

Surveyor ``.asc``/``.crd`` exports carry one row per shot as
``PtNo, Northing, Easting, Elevation, Description``, where the **first token of
the description is the field code** (e.g. ``FBC BLM S1/4`` -> ``FBC``). Mapping
that code through the field-code taxonomy (:mod:`totali.fieldcodes`) yields the
point's **class label** (its destination layer) — i.e. supervised training data
for the advisory classifier (U1), and the seed for the human-in-the-loop
correction flywheel.

Confidential: real exports live under the gitignored ``totali/data/``. This module
only parses them and any dataset written out must land in that gitignored tree —
nothing here embeds client points, and tests use synthetic fixtures.
"""

from __future__ import annotations

import re
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional

from totali.fieldcodes import FieldCodeTable

# First code-like token in a description (tolerates leading quotes/space, e.g.
# '"""FND","31544"""' -> 'FND', 'STK146284 FRAC' -> 'STK146284').
_CODE_RE = re.compile(r'[\s"\']*([A-Za-z0-9_/-]+)')


@dataclass(frozen=True)
class LabeledPoint:
    point_id: str
    northing: float
    easting: float
    elevation: float
    field_code: str
    raw_description: str
    layer: Optional[str]  # class label via taxonomy; None when code is unknown

    @property
    def is_labeled(self) -> bool:
        return self.layer is not None


def _field_code(description: str) -> str:
    match = _CODE_RE.match(description or "")
    return match.group(1) if match else ""


def parse_asc(path: Path | str, table: FieldCodeTable) -> list[LabeledPoint]:
    """Parse one ``.asc``/``.crd`` export into labeled points.

    Splits on the first four commas so descriptions may contain commas/quotes.
    Malformed or non-numeric rows are skipped (defensive against headers/blanks).
    """
    path = Path(path)
    points: list[LabeledPoint] = []
    with path.open(encoding="utf-8", errors="replace") as f:
        for raw in f:
            line = raw.strip()
            if not line:
                continue
            parts = line.split(",", 4)
            if len(parts) < 5:
                continue
            pid, n, e, z, desc = (p.strip() for p in parts)
            try:
                northing, easting, elevation = float(n), float(e), float(z)
            except ValueError:
                continue  # header row or non-coordinate line
            code = _field_code(desc)
            points.append(
                LabeledPoint(
                    point_id=pid,
                    northing=northing,
                    easting=easting,
                    elevation=elevation,
                    field_code=code,
                    raw_description=desc.strip('"'),
                    layer=table.layer_for(code),
                )
            )
    return points


@dataclass
class LabeledDataset:
    points: list[LabeledPoint]

    def __len__(self) -> int:
        return len(self.points)

    def labeled(self) -> list[LabeledPoint]:
        return [p for p in self.points if p.is_labeled]

    def unknown(self) -> list[LabeledPoint]:
        return [p for p in self.points if not p.is_labeled]

    def unknown_codes(self) -> set[str]:
        return {p.field_code for p in self.points if not p.is_labeled}

    def classes(self) -> tuple[str, ...]:
        return tuple(sorted({p.layer for p in self.labeled()}))

    def class_counts(self) -> dict[str, int]:
        return dict(Counter(p.layer for p in self.labeled()))

    def split(self, eval_frac: float = 0.2) -> tuple["LabeledDataset", "LabeledDataset"]:
        """Deterministic, per-class (stratified) train/eval split.

        No RNG: within each class, points are sorted by id and the first
        ``eval_frac`` go to eval, so the split is reproducible run-to-run — a
        defensibility property (same inputs -> same dataset).
        """
        if not 0.0 <= eval_frac < 1.0:
            raise ValueError("eval_frac must be in [0, 1)")
        by_class: dict[str, list[LabeledPoint]] = defaultdict(list)
        for p in self.labeled():
            by_class[p.layer].append(p)
        train: list[LabeledPoint] = []
        evalset: list[LabeledPoint] = []
        for pts in by_class.values():
            ordered = sorted(pts, key=lambda p: (len(p.point_id), p.point_id))
            n_eval = int(len(ordered) * eval_frac)
            evalset.extend(ordered[:n_eval])
            train.extend(ordered[n_eval:])
        return LabeledDataset(train), LabeledDataset(evalset)


def build_labeled_dataset(
    asc_paths: Iterable[Path | str],
    table: FieldCodeTable,
) -> LabeledDataset:
    """Build a combined labeled dataset from one or more coded exports."""
    points: list[LabeledPoint] = []
    for p in asc_paths:
        points.extend(parse_asc(p, table))
    return LabeledDataset(points)
