"""G-7/G-8 coverage: GEOID allowlist enforcement and PROJ audit metadata."""

import json
from pathlib import Path

import pytest

from totali.geodetic.gatekeeper import GeodeticGatekeeper


@pytest.fixture
def base_config():
    return {
        "allowed_crs": ["EPSG:2231"],
        "reject_on_missing_crs": True,
        "geoid_model": "GEOID18",
        "allowed_geoid_models": ["GEOID18"],
        "elevation_unit": "US_survey_foot",
        "unit_tolerance_ft": 0.01,
    }


@pytest.fixture
def gatekeeper_with_audit(audit_logger, base_config):
    gk = GeodeticGatekeeper(base_config, audit_logger)
    return gk, audit_logger


def _assert_proj_keys(payload: dict, gk: GeodeticGatekeeper) -> None:
    ctx = gk._proj_runtime_context()
    assert payload["pyproj_version"] == ctx["pyproj_version"]
    assert payload["proj_version"] == ctx["proj_version"]


class TestGeoidAllowlist:
    def test_geoid18_validated_without_sidecar(self, gatekeeper_with_audit):
        gk, audit = gatekeeper_with_audit
        errors, resolved = gk._validate_geoid(Path("sample.las"), None)
        assert errors == []
        assert resolved == "GEOID18"
        events = audit.get_events("geoid_validated")
        assert len(events) == 1
        payload = events[0]["data"]
        assert payload["resolved_geoid"] == "GEOID18"
        assert payload["declared_geoid"] is None
        assert payload["allowlist"] == ["GEOID18"]
        _assert_proj_keys(payload, gk)

    def test_declared_geoid18_accepted(self, gatekeeper_with_audit, tmp_path):
        gk, audit = gatekeeper_with_audit
        las_path = tmp_path / "site.las"
        las_path.write_bytes(b"\x00")
        sidecar = tmp_path / "site_geodetic_meta.json"
        sidecar.write_text(json.dumps({"geoid_model": "geoid18"}))

        errors, resolved = gk._validate_geoid(las_path, gk._read_declared_geoid(las_path))
        assert errors == []
        assert resolved == "GEOID18"
        assert audit.get_events("geoid_validated")

    def test_unsupported_geoid_rejected(self, gatekeeper_with_audit, tmp_path):
        gk, audit = gatekeeper_with_audit
        las_path = tmp_path / "bad.las"
        las_path.write_bytes(b"\x00")

        errors, resolved = gk._validate_geoid(las_path, "GEOID12")
        assert errors
        assert resolved is None
        assert "GEOID12" in errors[0]
        rejected = audit.get_events("geoid_rejected")
        assert len(rejected) == 1
        payload = rejected[0]["data"]
        assert payload["declared_geoid"] == "GEOID12"
        assert payload["reason"] == "geoid_not_allowed"
        assert payload["allowlist"] == ["GEOID18"]
        _assert_proj_keys(payload, gk)
        assert not audit.get_events("geoid_validated")

    def test_reject_helper_payload_shape(self, gatekeeper_with_audit):
        gk, audit = gatekeeper_with_audit
        gk._reject_unsupported_geoid(Path("x.las"), "GEOID09", "GEOID18")
        payload = audit.get_events("geoid_rejected")[0]["data"]
        assert set(payload.keys()) >= {
            "file",
            "declared_geoid",
            "expected_geoid",
            "allowlist",
            "reason",
            "pyproj_version",
            "proj_version",
        }


class TestProjAuditContextOnOtherEvents:
    def test_unit_validated_includes_proj_versions(self, gatekeeper_with_audit):
        gk, audit = gatekeeper_with_audit
        from totali.pipeline.models import CRSMetadata

        meta = CRSMetadata(
            epsg_code=2231,
            horizontal_unit="US_survey_foot",
            vertical_unit="US_survey_foot",
        )
        errors = gk._validate_units(meta, Path("good.las"))
        assert errors == []
        _assert_proj_keys(audit.get_events("unit_validated")[0]["data"], gk)

    def test_extract_crs_emits_geoid_validated(self, gatekeeper_with_audit):
        gk, audit = gatekeeper_with_audit
        import laspy

        las = laspy.read("fake")
        las.vlrs = []
        meta = gk._extract_crs(las, Path("no_crs.las"))
        assert meta.is_valid is False
        assert audit.get_events("geoid_validated")
        _assert_proj_keys(audit.get_events("geoid_validated")[0]["data"], gk)
