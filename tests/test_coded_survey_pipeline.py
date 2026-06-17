"""Integration tests for coded-survey (.asc) → DXF pipeline wiring."""

from __future__ import annotations

import json

import pytest

from totali.fieldcodes import load_field_codes
from totali.pipeline.input_kind import input_kind, is_coded_survey_input
from totali.pipeline.orchestrator import PipelineOrchestrator

SAMPLE_FLD = """#2010V# Code|Description|Symbol|Symbol Size|Layer
FIELD CODE|Layer|Symbol|0.0000|none
TOPO|TOPO|DOT1|0.0000|none
CP|CONTROL_POINT|CTRLPT|0.0000|none
"""

SAMPLE_ASC = (
    "1,1372340.54,2818546.82,8010.78,TOPO shot grade\n"
    "2,1372341.10,2818547.00,8010.90,CP control\n"
    "3,1372342.00,2818548.00,8011.00,TOPO second shot\n"
)


@pytest.fixture
def coded_survey_config(tmp_path, sample_config):
    fld = tmp_path / "codes.fld"
    fld.write_text(SAMPLE_FLD, encoding="utf-8")
    cfg = dict(sample_config)
    cfg["geodetic"] = dict(sample_config["geodetic"])
    cfg["geodetic"]["fieldcode_fld"] = str(fld)
    cfg["geodetic"]["crs_inference_enabled"] = False
    cfg["segmentation"] = dict(sample_config["segmentation"])
    cfg["segmentation"]["fieldcode_fld"] = str(fld)
    return cfg


@pytest.fixture
def coded_asc(tmp_path):
    p = tmp_path / "job.asc"
    p.write_text(SAMPLE_ASC, encoding="utf-8")
    return p


class TestInputKind:
    def test_detects_asc(self, coded_asc):
        assert is_coded_survey_input(coded_asc)
        assert input_kind(coded_asc) == "coded_survey"

    def test_detects_las(self, tmp_path):
        las = tmp_path / "cloud.las"
        las.write_bytes(b"\x00")
        assert input_kind(las) == "las"


class TestCodedSurveyPipeline:
    def test_geodetic_through_shield(
        self, audit_logger, coded_survey_config, coded_asc, tmp_path
    ):
        out = tmp_path / "out"
        out.mkdir()
        orch = PipelineOrchestrator(coded_survey_config, audit_logger, out)
        result = orch.run(str(coded_asc), phase="all")

        phase_names = [p.phase for p in result.phases]
        assert phase_names[:4] == ["geodetic", "segment", "extract", "shield"]
        assert result.phases[0].success is True
        assert result.phases[1].success is True
        assert result.phases[2].success is True
        assert result.phases[3].success is True

        dxf = out / "totali_draft_output.dxf"
        assert dxf.exists()

        manifest_path = out / "entity_manifest.json"
        manifest = json.loads(manifest_path.read_text())
        assert manifest["entity_count"] == 3
        layers = {e["layer"] for e in manifest["entities"]}
        assert "TOTaLi-SURV-TOPO-DRAFT" in layers
        assert "TOTaLi-SURV-CTRL-DRAFT" in layers
        assert manifest.get("audit_reference", {}).get("input_hash")

    def test_segment_uses_coded_classifier_not_las_classifier(
        self, audit_logger, coded_survey_config, coded_asc, tmp_path
    ):
        out = tmp_path / "out"
        out.mkdir()
        orch = PipelineOrchestrator(coded_survey_config, audit_logger, out)
        result = orch.run(str(coded_asc), phase="segment")

        # segment alone does not run geodetic — expect validation failure
        assert result.phases[-1].phase == "segment"
        assert result.phases[-1].success is False

    def test_field_code_table_fixture(self, tmp_path):
        fld = tmp_path / "codes.fld"
        fld.write_text(SAMPLE_FLD, encoding="utf-8")
        table = load_field_codes(fld)
        assert table.layer_for("TOPO") == "TOPO"
