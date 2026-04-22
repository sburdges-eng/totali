"""C-4: cad_shielding.format is validated at construction.

DXF = supported. DWG = stub (dwg-tool-parser writer not wired). DGN = deferred.
Unknown = ValueError. Failing at __init__ prevents a misconfigured deployment
from starting the pipeline.
"""

import pytest

from totali.cad_shielding.shield import CADShield, UnsupportedCADFormat, _FORMAT_STATUS


def _cfg(fmt):
    return {
        "format": fmt,
        "geometry_healing": {},
        "layer_mapping": {"ground_surface": "TOTaLi-SURV-DTM-DRAFT"},
    }


class TestFormatAccepted:
    def test_dxf_accepted(self, audit_logger):
        shield = CADShield(_cfg("dxf"), audit_logger)
        assert shield.format == "dxf"

    def test_dxf_case_insensitive(self, audit_logger):
        shield = CADShield(_cfg("DXF"), audit_logger)
        assert shield.format == "dxf"


class TestFormatRejected:
    def test_dwg_rejected_as_stub(self, audit_logger):
        with pytest.raises(UnsupportedCADFormat) as exc:
            CADShield(_cfg("dwg"), audit_logger)
        assert "dwg" in str(exc.value).lower()
        assert "stub" in str(exc.value).lower()

    def test_dgn_rejected_as_deferred(self, audit_logger):
        with pytest.raises(UnsupportedCADFormat) as exc:
            CADShield(_cfg("dgn"), audit_logger)
        assert "deferred" in str(exc.value).lower()

    def test_unknown_format_is_value_error(self, audit_logger):
        with pytest.raises(ValueError) as exc:
            CADShield(_cfg("svg"), audit_logger)
        assert "not recognised" in str(exc.value) or "not recognized" in str(exc.value)

    def test_unknown_format_is_not_unsupported_cad_format(self, audit_logger):
        """Unknown ≠ stub/deferred — caller may handle differently."""
        with pytest.raises(ValueError) as exc:
            CADShield(_cfg("pdf"), audit_logger)
        assert not isinstance(exc.value, UnsupportedCADFormat)


class TestFormatStatusTable:
    def test_table_has_expected_entries(self):
        assert _FORMAT_STATUS == {
            "dxf": "supported",
            "dwg": "stub",
            "dgn": "deferred",
        }

    def test_dxf_is_the_only_supported_format(self):
        supported = [f for f, s in _FORMAT_STATUS.items() if s == "supported"]
        assert supported == ["dxf"]


class TestDefaultFormat:
    def test_default_is_dxf(self, audit_logger):
        cfg = {"geometry_healing": {}, "layer_mapping": {"g": "TOTaLi-SURV-DTM-DRAFT"}}
        shield = CADShield(cfg, audit_logger)
        assert shield.format == "dxf"
