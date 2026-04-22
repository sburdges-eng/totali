"""G-3 coverage: US survey foot unit validation, metric rejection, audit events."""

from pathlib import Path

import pytest

from totali.geodetic.gatekeeper import GeodeticGatekeeper
from totali.pipeline.models import CRSMetadata


@pytest.fixture
def base_config():
    return {
        "allowed_crs": ["EPSG:2231"],
        "reject_on_missing_crs": True,
        "geoid_model": "GEOID18",
        "elevation_unit": "US_survey_foot",
        "unit_tolerance_ft": 0.01,
    }


@pytest.fixture
def gatekeeper_with_audit(audit_logger, base_config):
    gk = GeodeticGatekeeper(base_config, audit_logger)
    return gk, audit_logger


def _meta(h_unit: str, v_unit: str) -> CRSMetadata:
    return CRSMetadata(
        epsg_code=2231,
        horizontal_unit=h_unit,
        vertical_unit=v_unit,
    )


class TestMetricRejection:
    def test_meter_rejected(self, gatekeeper_with_audit):
        gk, audit = gatekeeper_with_audit
        errors = gk._validate_units(_meta("meter", "meter"), Path("sample.las"))
        assert errors
        assert any("Metric" in e for e in errors)
        events = audit.get_events("unit_rejected")
        assert len(events) == 1
        payload = events[0]["data"]
        assert payload["reason"] == "metric_not_allowed"
        assert payload["expected_unit"] == "us_survey_foot"
        assert payload["horizontal_unit"] == "meter"
        assert payload["vertical_unit"] == "meter"
        assert payload["tolerance_ft"] == 0.01

    @pytest.mark.parametrize("alias", ["metre", "m", "meters", "metres"])
    def test_metric_aliases_rejected(self, gatekeeper_with_audit, alias):
        gk, audit = gatekeeper_with_audit
        errors = gk._validate_units(_meta(alias, alias), Path("f.las"))
        assert errors
        assert audit.get_events("unit_rejected")

    def test_mixed_metric_horizontal_only(self, gatekeeper_with_audit):
        gk, audit = gatekeeper_with_audit
        errors = gk._validate_units(
            _meta("meter", "US_survey_foot"), Path("mix.las")
        )
        assert errors
        assert len(audit.get_events("unit_rejected")) == 1
        assert audit.get_events("unit_rejected")[0]["data"]["reason"] == "metric_not_allowed"


class TestUSSurveyFootAccepted:
    @pytest.mark.parametrize(
        "alias", ["US_survey_foot", "us survey foot", "ftus", "USFT"]
    )
    def test_canonical_aliases_pass(self, gatekeeper_with_audit, alias):
        gk, audit = gatekeeper_with_audit
        errors = gk._validate_units(_meta(alias, alias), Path("good.las"))
        assert errors == []
        events = audit.get_events("unit_validated")
        assert len(events) == 1
        assert events[0]["data"]["canonical_unit"] == "us_survey_foot"
        assert events[0]["data"]["tolerance_ft"] == 0.01


class TestInternationalFootMismatch:
    @pytest.mark.parametrize("alias", ["foot", "feet", "ft", "international_foot"])
    def test_international_foot_rejected_as_mismatch(
        self, gatekeeper_with_audit, alias
    ):
        gk, audit = gatekeeper_with_audit
        errors = gk._validate_units(_meta(alias, alias), Path("intl.las"))
        assert errors
        events = audit.get_events("unit_rejected")
        assert events
        assert all(e["data"]["reason"] == "unit_mismatch" for e in events)
        assert all(e["data"]["expected_unit"] == "us_survey_foot" for e in events)


class TestConfigDrivenTolerance:
    def test_tolerance_read_from_config(self, audit_logger):
        config = {
            "allowed_crs": ["EPSG:2231"],
            "reject_on_missing_crs": True,
            "geoid_model": "GEOID18",
            "elevation_unit": "US_survey_foot",
            "unit_tolerance_ft": 0.005,
        }
        gk = GeodeticGatekeeper(config, audit_logger)
        assert gk.unit_tolerance_ft == pytest.approx(0.005)
        gk._validate_units(_meta("meter", "meter"), Path("x.las"))
        payload = audit_logger.get_events("unit_rejected")[0]["data"]
        assert payload["tolerance_ft"] == pytest.approx(0.005)

    def test_tolerance_default_when_missing(self, audit_logger):
        config = {
            "allowed_crs": ["EPSG:2231"],
            "reject_on_missing_crs": True,
            "geoid_model": "GEOID18",
            "elevation_unit": "US_survey_foot",
        }
        gk = GeodeticGatekeeper(config, audit_logger)
        assert gk.unit_tolerance_ft == pytest.approx(0.01)


class TestNoHardcodedLiterals:
    """Canonical unit must come from config; US_survey_foot is not wired in as literal."""

    def test_canonical_comes_from_config(self, audit_logger):
        config = {
            "allowed_crs": ["EPSG:2231"],
            "reject_on_missing_crs": True,
            "geoid_model": "GEOID18",
            "elevation_unit": "custom_unit_xyz",
            "unit_tolerance_ft": 0.01,
        }
        gk = GeodeticGatekeeper(config, audit_logger)
        errors = gk._validate_units(
            _meta("custom_unit_xyz", "custom_unit_xyz"), Path("c.las")
        )
        assert errors == []
        errors2 = gk._validate_units(
            _meta("US_survey_foot", "US_survey_foot"), Path("c2.las")
        )
        assert errors2, "Non-canonical unit must be rejected when canonical is customised"


class TestRejectMetricHelper:
    def test_reject_metric_payload_shape(self, gatekeeper_with_audit):
        gk, audit = gatekeeper_with_audit
        gk._reject_metric(
            Path("direct.las"), "meter", "meter", "us_survey_foot"
        )
        events = audit.get_events("unit_rejected")
        assert len(events) == 1
        data = events[0]["data"]
        assert set(data.keys()) >= {
            "file",
            "horizontal_unit",
            "vertical_unit",
            "expected_unit",
            "tolerance_ft",
            "reason",
        }
        assert data["reason"] == "metric_not_allowed"
        assert data["file"] == "direct.las"
