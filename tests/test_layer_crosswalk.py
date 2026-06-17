"""Tests for the firm-layer → TOTaLi DRAFT-layer crosswalk.

Critically cross-checks every crosswalk output against the shield's *own* §1.3
validator, so the crosswalk can never drift from the layer-name invariant.
"""

from __future__ import annotations

import pytest

from totali.cad_shielding.layer_crosswalk import (
    FIRM_LAYER_CROSSWALK,
    UNCODED_LAYER,
    draft_layer_for,
    firm_layer_to_draft,
)
from totali.cad_shielding.shield import CADShield, NonConformingLayerName


class TestCrosswalkConformance:
    def test_all_curated_outputs_pass_shield_validator(self):
        # The shield validator is the source of truth for §1.3; if any curated
        # name fails it, the crosswalk is wrong.
        mapping = {k: v for k, v in FIRM_LAYER_CROSSWALK.items()}
        CADShield._validate_layer_mapping(mapping)  # raises if non-conforming

    def test_uncoded_layer_is_qa(self):
        assert UNCODED_LAYER.startswith("TOTaLi-QA-")
        CADShield._validate_layer_mapping({"x": UNCODED_LAYER})


class TestMapping:
    def test_curated_layer_uses_clean_name(self):
        assert firm_layer_to_draft("TOPO") == "TOTaLi-SURV-TOPO-DRAFT"
        assert firm_layer_to_draft("CULVERT") == "TOTaLi-PLAN-CULVERT-DRAFT"

    def test_unknown_layer_sanitized_and_conforms(self):
        out = firm_layer_to_draft("FOUND_REBAR_W/_ALUMINUM_CAP", discipline="SURV")
        # In the curated map -> clean name.
        assert out == "TOTaLi-SURV-REBARALUM-DRAFT"

    def test_truly_unknown_layer_falls_back_and_conforms(self):
        out = firm_layer_to_draft("Some Weird/Layer Name!", discipline="PLAN")
        assert out.startswith("TOTaLi-PLAN-")
        assert out.endswith("-DRAFT")
        CADShield._validate_layer_mapping({"x": out})

    def test_draft_layer_for_uncoded(self):
        assert draft_layer_for(None) == UNCODED_LAYER
        assert draft_layer_for("") == UNCODED_LAYER

    def test_draft_layer_for_known(self):
        assert draft_layer_for("CREEK") == "TOTaLi-SURV-CREEK-DRAFT"

    def test_shield_rejects_raw_firm_layer(self):
        # Proves the crosswalk is necessary: a raw firm layer is rejected.
        with pytest.raises(NonConformingLayerName):
            CADShield._validate_layer_mapping({"topo": "TOPO"})
