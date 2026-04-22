"""G-9: _validate_units is deterministic — identical inputs produce identical events."""

from pathlib import Path

import pytest

from totali.geodetic.gatekeeper import GeodeticGatekeeper
from totali.pipeline.models import CRSMetadata


@pytest.fixture
def gatekeeper(audit_logger):
    config = {
        "allowed_crs": ["EPSG:2231"],
        "reject_on_missing_crs": True,
        "geoid_model": "GEOID18",
        "elevation_unit": "US_survey_foot",
        "unit_tolerance_ft": 0.01,
    }
    return GeodeticGatekeeper(config, audit_logger), audit_logger


def _meta(h: str, v: str) -> CRSMetadata:
    return CRSMetadata(epsg_code=2231, horizontal_unit=h, vertical_unit=v)


class TestValidateUnitsDeterminism:
    def test_same_input_same_payload(self, gatekeeper):
        gk, audit = gatekeeper
        gk._validate_units(_meta("meter", "meter"), Path("same.las"))
        gk._validate_units(_meta("meter", "meter"), Path("same.las"))
        events = [e["data"] for e in audit.get_events("unit_rejected")]
        assert len(events) == 2
        # Remove any time-varying fields — payload content is deterministic.
        assert events[0] == events[1]

    def test_order_preserved(self, gatekeeper):
        gk, audit = gatekeeper
        for i in range(5):
            gk._validate_units(
                _meta("US_survey_foot", "US_survey_foot"),
                Path(f"f{i}.las"),
            )
        events = audit.get_events("unit_validated")
        files = [e["data"]["file"] for e in events]
        assert files == [f"f{i}.las" for i in range(5)]
