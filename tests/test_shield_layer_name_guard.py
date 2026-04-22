"""C-3: Layer names conform to TOTaLi-<DISC>-<FEAT>-DRAFT (TOTaLi-QA-* exempt).

Invariant: no AI/algorithmic output is ever written to a non-DRAFT, non-QA
layer. This test sweeps the config/pipeline.yaml layer_mapping and any layer
name injected via SurveyorLinter/CADShield config.
"""

import re

import pytest
import yaml

from totali.cad_shielding.shield import CADShield


LAYER_RE = re.compile(
    r"^TOTaLi-[A-Z0-9]+(?:-[A-Z0-9_]+)+-DRAFT$|^TOTaLi-QA-[A-Z0-9_-]+$"
)


def _load_pipeline_yaml():
    from pathlib import Path

    path = Path(__file__).resolve().parents[1] / "config" / "pipeline.yaml"
    with path.open() as f:
        return yaml.safe_load(f)


class TestPipelineYamlLayerNames:
    def test_all_layers_conform(self):
        cfg = _load_pipeline_yaml()
        mapping = cfg.get("cad_shielding", {}).get("layer_mapping", {})
        assert mapping, "pipeline.yaml must define cad_shielding.layer_mapping"
        bad = [name for name in mapping.values() if not LAYER_RE.match(name)]
        assert not bad, (
            f"Non-conforming layer names: {bad}. "
            "Must match TOTaLi-<DISC>-<FEAT>-DRAFT or TOTaLi-QA-<name>."
        )

    def test_certified_layer_suffix_is_empty(self):
        cfg = _load_pipeline_yaml()
        suffix = cfg.get("cad_shielding", {}).get("certified_layer_suffix")
        assert suffix == "", (
            "certified_layer_suffix must be empty string — promotion is a "
            "manual step that strips -DRAFT; a non-empty suffix would allow "
            "auto-promotion side-effects."
        )


class TestShieldConfigLayerNames:
    def test_shield_accepts_conforming_names(self, audit_logger):
        config = {
            "format": "dxf",
            "geometry_healing": {},
            "layer_mapping": {
                "ground_surface": "TOTaLi-SURV-DTM-DRAFT",
                "occlusion_zones": "TOTaLi-QA-OCCLUSION",
            },
        }
        shield = CADShield(config, audit_logger)
        for name in shield.layer_map.values():
            assert LAYER_RE.match(name), f"{name} failed layer-name regex"


class TestShieldRejectsBadLayerNames:
    """C-3 enforcement: __init__ refuses non-conforming names."""

    def test_missing_draft_suffix_rejected(self, audit_logger):
        from totali.cad_shielding.shield import CADShield, NonConformingLayerName

        config = {
            "format": "dxf",
            "geometry_healing": {},
            "layer_mapping": {"ground_surface": "TOTaLi-SURV-DTM"},
        }
        with pytest.raises(NonConformingLayerName) as exc:
            CADShield(config, audit_logger)
        assert "TOTaLi-SURV-DTM" in str(exc.value)

    def test_arbitrary_name_rejected(self, audit_logger):
        from totali.cad_shielding.shield import CADShield, NonConformingLayerName

        config = {
            "format": "dxf",
            "geometry_healing": {},
            "layer_mapping": {"x": "MyOwnLayer"},
        }
        with pytest.raises(NonConformingLayerName):
            CADShield(config, audit_logger)

    def test_lowercase_disc_rejected(self, audit_logger):
        from totali.cad_shielding.shield import CADShield, NonConformingLayerName

        config = {
            "format": "dxf",
            "geometry_healing": {},
            "layer_mapping": {"x": "TOTaLi-surv-DTM-DRAFT"},
        }
        with pytest.raises(NonConformingLayerName):
            CADShield(config, audit_logger)

    def test_qa_layer_accepted(self, audit_logger):
        config = {
            "format": "dxf",
            "geometry_healing": {},
            "layer_mapping": {
                "occlusion_zones": "TOTaLi-QA-OCCLUSION",
                "uncertainty_flags": "TOTaLi-QA-FLAGS",
            },
        }
        # Should not raise.
        CADShield(config, audit_logger)

    def test_empty_mapping_accepted(self, audit_logger):
        config = {"format": "dxf", "geometry_healing": {}, "layer_mapping": {}}
        # Empty mapping is degenerate but not a layer-name violation; downstream
        # phases will fail loudly when they need a name. C-3 only guards names
        # that DO exist.
        CADShield(config, audit_logger)
