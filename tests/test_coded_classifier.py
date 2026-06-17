"""Tests for the deterministic coded-point classifier."""

from __future__ import annotations

from totali.segmentation.coded import (
    CodedClassification,
    classify_coded_points,
    draft_layer_counts,
)
from totali.segmentation.dataset import LabeledPoint


def _pt(pid, code, layer):
    return LabeledPoint(
        point_id=pid, northing=1.0, easting=2.0, elevation=3.0,
        field_code=code, raw_description=f"{code} note", layer=layer,
    )


class TestClassifyCoded:
    def test_maps_firm_layer_to_draft_layer(self):
        pts = [_pt("1", "TOPO", "TOPO"), _pt("2", "CP", "CONTROL_POINT")]
        out = classify_coded_points(pts)
        assert isinstance(out[0], CodedClassification)
        assert out[0].draft_layer == "TOTaLi-SURV-TOPO-DRAFT"
        assert out[1].draft_layer == "TOTaLi-SURV-CTRL-DRAFT"

    def test_authoritative_by_default(self):
        out = classify_coded_points([_pt("1", "TOPO", "TOPO")])
        assert out[0].authoritative is True  # surveyor-coded, not advisory

    def test_unknown_code_routes_to_qa_uncoded(self):
        out = classify_coded_points([_pt("9", "ZZZ", None)])
        assert out[0].firm_layer is None
        assert out[0].draft_layer == "TOTaLi-QA-UNCODED"

    def test_draft_layer_counts(self):
        pts = [
            _pt("1", "TOPO", "TOPO"),
            _pt("2", "TOP", "TOPO"),
            _pt("3", "CP", "CONTROL_POINT"),
            _pt("4", "ZZZ", None),
        ]
        counts = draft_layer_counts(classify_coded_points(pts))
        assert counts == {
            "TOTaLi-SURV-TOPO-DRAFT": 2,
            "TOTaLi-SURV-CTRL-DRAFT": 1,
            "TOTaLi-QA-UNCODED": 1,
        }
