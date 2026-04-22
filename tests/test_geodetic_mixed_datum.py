"""G-4: `reject_on_mixed_datum` config plumbing.

Current single-file ingestion doesn't have a multi-file mixed-datum path yet
(that comes in a later plan step). This test pins the config contract so the
flag is wired and defaults correctly.
"""

import pytest

from totali.geodetic.gatekeeper import GeodeticGatekeeper


@pytest.fixture
def _mk(audit_logger):
    def _build(overrides=None):
        base = {
            "allowed_crs": ["EPSG:2231"],
            "reject_on_missing_crs": True,
            "geoid_model": "GEOID18",
            "elevation_unit": "US_survey_foot",
        }
        if overrides:
            base.update(overrides)
        return GeodeticGatekeeper(base, audit_logger)

    return _build


class TestMixedDatumFlag:
    def test_default_is_true(self, _mk):
        gk = _mk()
        assert gk.reject_mixed_datum is True

    def test_false_honored_from_config(self, _mk):
        gk = _mk({"reject_on_mixed_datum": False})
        assert gk.reject_mixed_datum is False

    def test_true_honored_from_config(self, _mk):
        gk = _mk({"reject_on_mixed_datum": True})
        assert gk.reject_mixed_datum is True


class TestRejectMissingCRSFlag:
    def test_default_is_true(self, _mk):
        gk = _mk()
        assert gk.reject_missing_crs is True

    def test_false_switches_to_inference_mode(self, _mk):
        gk = _mk({"reject_on_missing_crs": False})
        assert gk.reject_missing_crs is False
        # And inference should therefore default to enabled.
        assert gk.crs_inference_enabled is True


class TestInferenceThreshold:
    def test_default_threshold(self, _mk):
        gk = _mk({"reject_on_missing_crs": False})
        assert gk.crs_confidence_threshold == 0.8

    def test_threshold_read_from_config(self, _mk):
        gk = _mk({
            "reject_on_missing_crs": False,
            "crs_inference": {"enabled": True, "confidence_threshold": 0.95},
        })
        assert gk.crs_confidence_threshold == 0.95
