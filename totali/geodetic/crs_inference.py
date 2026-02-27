"""
Spatial heuristic CRS inference for missing metadata.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Tuple

import numpy as np


US_SURVEY_FOOT_TO_METER = 0.3048006096012192
INTERNATIONAL_FOOT_TO_METER = 0.3048


@dataclass
class EPSGCandidate:
    epsg: int
    name: str
    bounds: Tuple[float, float, float, float]  # min_x, min_y, max_x, max_y
    unit: str
    confidence: float = 0.0


COLORADO_SPCS = [
    EPSGCandidate(2231, "NAD83 CO North (ftUS)", (100000, 0, 950000, 500000), "US_survey_foot"),
    EPSGCandidate(2232, "NAD83 CO Central (ftUS)", (100000, 0, 950000, 500000), "US_survey_foot"),
    EPSGCandidate(2233, "NAD83 CO South (ftUS)", (100000, 0, 950000, 500000), "US_survey_foot"),
    EPSGCandidate(6428, "NAD83(2011) CO North (ftUS)", (100000, 0, 950000, 500000), "US_survey_foot"),
    EPSGCandidate(6430, "NAD83(2011) CO Central (ftUS)", (100000, 0, 950000, 500000), "US_survey_foot"),
    EPSGCandidate(6432, "NAD83(2011) CO South (ftUS)", (100000, 0, 950000, 500000), "US_survey_foot"),
]


class CRSInferenceEngine:
    def __init__(self, candidates: List[EPSGCandidate] | None = None):
        self.candidates = candidates or COLORADO_SPCS

    def infer_from_bounds(self, bounds_min: np.ndarray, bounds_max: np.ndarray) -> List[EPSGCandidate]:
        """Infer candidate EPSG codes from XY coordinate bounds."""
        results: List[EPSGCandidate] = []
        data_min_x, data_min_y = float(bounds_min[0]), float(bounds_min[1])
        data_max_x, data_max_y = float(bounds_max[0]), float(bounds_max[1])

        for cand in self.candidates:
            cb = cand.bounds
            x_in = cb[0] <= data_min_x <= cb[2] and cb[0] <= data_max_x <= cb[2]
            y_in = cb[1] <= data_min_y <= cb[3] and cb[1] <= data_max_y <= cb[3]
            if not (x_in and y_in):
                continue

            x_range = max(cb[2] - cb[0], 1.0)
            y_range = max(cb[3] - cb[1], 1.0)
            data_x_range = max(data_max_x - data_min_x, 0.0)
            data_y_range = max(data_max_y - data_min_y, 0.0)
            x_ratio = min(data_x_range / x_range, 1.0)
            y_ratio = min(data_y_range / y_range, 1.0)

            # copy candidate to avoid mutating global confidence state
            results.append(
                EPSGCandidate(
                    epsg=cand.epsg,
                    name=cand.name,
                    bounds=cand.bounds,
                    unit=cand.unit,
                    confidence=(x_ratio + y_ratio) / 2.0,
                )
            )

        return sorted(results, key=lambda c: c.confidence, reverse=True)

    def infer_unit_scale(self, bounds_min: np.ndarray, bounds_max: np.ndarray) -> tuple[str, float]:
        """
        Infer likely units from coordinate magnitude.
        Returns (unit_name, factor_to_meters).
        """
        max_coord = float(max(abs(bounds_max[0]), abs(bounds_max[1]), abs(bounds_min[0]), abs(bounds_min[1])))
        if 50000 < max_coord < 1000000:
            return "US_survey_foot", US_SURVEY_FOOT_TO_METER
        if 100 < max_coord < 5000:
            return "meter", 1.0
        return "unknown", 1.0

    def requires_human_review(
        self,
        candidates: List[EPSGCandidate],
        *,
        confidence_threshold: float = 0.8,
    ) -> bool:
        """True when inference is absent or ambiguous."""
        if not candidates:
            return True
        if len(candidates) == 1:
            return candidates[0].confidence < confidence_threshold
        if candidates[0].confidence - candidates[1].confidence < 0.2:
            return True
        return candidates[0].confidence < confidence_threshold
