"""Tests for CRS inference engine."""

import numpy as np

from totali.geodetic.crs_inference import (
    CRSInferenceEngine,
    EPSGCandidate,
    COLORADO_SPCS,
    US_SURVEY_FOOT_TO_METER,
)


class TestCRSInferenceEngine:
    def test_infers_colorado_candidates_from_bounds(self):
        engine = CRSInferenceEngine()
        bounds_min = np.array([200000.0, 10000.0, 0.0])
        bounds_max = np.array([250000.0, 60000.0, 10.0])
        candidates = engine.infer_from_bounds(bounds_min, bounds_max)
        assert len(candidates) >= 1
        assert candidates[0].epsg in {2231, 2232, 2233, 6428, 6430, 6432}

    def test_returns_empty_when_out_of_known_bounds(self):
        engine = CRSInferenceEngine()
        bounds_min = np.array([0.0, 0.0, 0.0])
        bounds_max = np.array([1000.0, 1000.0, 10.0])
        candidates = engine.infer_from_bounds(bounds_min, bounds_max)
        assert candidates == []

    def test_infers_units(self):
        engine = CRSInferenceEngine()
        unit, scale = engine.infer_unit_scale(
            np.array([200000.0, 10000.0, 0.0]),
            np.array([250000.0, 60000.0, 10.0]),
        )
        assert unit == "US_survey_foot"
        assert scale > 0.3

    def test_review_required_for_ambiguous_or_low_confidence(self):
        engine = CRSInferenceEngine()
        bounds_min = np.array([200000.0, 10000.0, 0.0])
        bounds_max = np.array([201000.0, 11000.0, 10.0])  # tiny window -> low confidence
        candidates = engine.infer_from_bounds(bounds_min, bounds_max)
        assert engine.requires_human_review(candidates, confidence_threshold=0.8) is True

    def test_no_review_when_single_high_confidence(self):
        engine = CRSInferenceEngine()
        high = [EPSGCandidate(2231, "North", (0, 0, 1e6, 1e6), "US_survey_foot", confidence=0.95)]
        assert engine.requires_human_review(high, confidence_threshold=0.8) is False

    def test_review_required_when_empty_candidates(self):
        engine = CRSInferenceEngine()
        assert engine.requires_human_review([], confidence_threshold=0.5) is True

    def test_review_required_when_top_two_close_in_confidence(self):
        engine = CRSInferenceEngine()
        cands = [
            EPSGCandidate(2231, "A", (0, 0, 1, 1), "ft", confidence=0.75),
            EPSGCandidate(2232, "B", (0, 0, 1, 1), "ft", confidence=0.70),
        ]
        assert engine.requires_human_review(cands, confidence_threshold=0.8) is True

    def test_infer_unit_scale_meters(self):
        engine = CRSInferenceEngine()
        unit, scale = engine.infer_unit_scale(
            np.array([100.0, 200.0, 0.0]),
            np.array([500.0, 600.0, 10.0]),
        )
        assert unit == "meter"
        assert scale == 1.0

    def test_infer_from_bounds_returns_sorted_by_confidence_descending(self):
        engine = CRSInferenceEngine()
        bounds_min = np.array([150000.0, 50000.0, 0.0])
        bounds_max = np.array([800000.0, 400000.0, 10.0])
        candidates = engine.infer_from_bounds(bounds_min, bounds_max)
        assert len(candidates) >= 1
        confs = [c.confidence for c in candidates]
        assert confs == sorted(confs, reverse=True)

    def test_custom_candidates_list(self):
        custom = [EPSGCandidate(9999, "Custom", (0, 0, 100, 100), "meter", confidence=0.0)]
        engine = CRSInferenceEngine(candidates=custom)
        candidates = engine.infer_from_bounds(np.array([10.0, 10.0, 0.0]), np.array([90.0, 90.0, 0.0]))
        assert len(candidates) == 1
        assert candidates[0].epsg == 9999
        assert candidates[0].confidence > 0
