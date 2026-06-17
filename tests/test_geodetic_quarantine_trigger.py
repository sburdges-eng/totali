"""G-5/G-6: low-confidence CRS inference routes to quarantine (port 5050).

When ``auto_assign_high_confidence`` is enabled, a single-zone INFERRED CRS
with confidence below ``crs_confidence_threshold`` must halt the phase, write
the flask-free quarantine artifact, enqueue the operator UI, and emit
``crs_quarantined``. High-confidence inference (full extent inside the zone
envelope, score 0.9) may still auto-proceed with ``requires_review`` on the
audit record.
"""

from __future__ import annotations

import json

import pytest

from totali.geodetic.gatekeeper import GeodeticGatekeeper
from totali.pipeline.context import PipelineContext

_BASE_CFG = {
    "allowed_crs": ["EPSG:2231", "EPSG:2232", "EPSG:2233"],
    "reject_on_missing_crs": True,
    "geoid_model": "GEOID18",
    "elevation_unit": "US_survey_foot",
    "crs_inference_enabled": True,
    "crs_confidence_threshold": 0.8,
    "auto_assign_high_confidence": True,
    "quarantine_ui_port": 5050,
}

# Fake LAS bounds are ~[0, 1000] on X/Y (see conftest _FakeLasData).
_ZONE_FULL_CONTAIN_2231 = {
    "epsg": 2231,
    "name": "NAD83 Colorado North (ftUS)",
    "xy_min": [-1.0, -1.0],
    "xy_max": [2000.0, 2000.0],
}
# Centroid (~500, 500) is inside; corners spill outside -> confidence 0.6.
_ZONE_PARTIAL_2231 = {
    "epsg": 2231,
    "name": "NAD83 Colorado North (ftUS)",
    "xy_min": [400.0, 400.0],
    "xy_max": [600.0, 600.0],
}


def _mk_gatekeeper(audit_logger, **overrides):
    cfg = dict(_BASE_CFG)
    cfg.update(overrides)
    return GeodeticGatekeeper(cfg, audit_logger)


def _make_las(tmp_path):
    las = tmp_path / "survey.las"
    las.write_bytes(b"\x00" * 256)
    return las


def _run(gk, tmp_path):
    las = _make_las(tmp_path)
    out = tmp_path / "out"
    out.mkdir(exist_ok=True)
    ctx = PipelineContext(input_path=str(las), output_dir=out)
    return gk.run(ctx), out


class TestConfidenceThresholdConfig:
    def test_defaults_exposed_on_gatekeeper(self, audit_logger):
        gk = GeodeticGatekeeper(
            {
                "allowed_crs": ["EPSG:2231"],
                "reject_on_missing_crs": True,
                "geoid_model": "GEOID18",
                "elevation_unit": "US_survey_foot",
            },
            audit_logger,
        )
        assert gk.crs_confidence_threshold == 0.8
        assert gk.auto_assign_high_confidence is True
        assert gk.quarantine_ui_port == 5050


class TestLowConfidenceQuarantineTrigger:
    def test_partial_fit_routes_to_quarantine(self, audit_logger, tmp_path):
        gk = _mk_gatekeeper(
            audit_logger,
            jurisdiction_zones=[_ZONE_PARTIAL_2231],
        )
        result, out = _run(gk, tmp_path)

        assert result.success is False
        assert result.data.get("quarantined") is True
        assert result.data.get("quarantine_ui_port") == 5050
        assert result.data.get("confidence") == pytest.approx(0.6)

        artifact = out / "survey_crs_quarantine.json"
        assert artifact.exists()
        payload = json.loads(artifact.read_text())
        assert payload["status"] == "inferred"
        assert payload["candidates"][0]["confidence"] == pytest.approx(0.6)

        events = audit_logger.get_events("crs_quarantined")
        assert len(events) == 1
        assert len(audit_logger.get_events("crs_inferred")) == 0

    def test_high_confidence_single_zone_proceeds(self, audit_logger, tmp_path):
        gk = _mk_gatekeeper(
            audit_logger,
            jurisdiction_zones=[_ZONE_FULL_CONTAIN_2231],
        )
        result, _ = _run(gk, tmp_path)

        assert result.success is True
        assert result.data["crs"].epsg_code == 2231
        inferred = audit_logger.get_events("crs_inferred")
        assert len(inferred) == 1
        assert inferred[0]["data"]["confidence"] == pytest.approx(0.9)
        assert inferred[0]["data"]["requires_review"] is True


class TestQuarantineUIPortWiring:
    def test_low_confidence_enqueues_for_operator_ui(self, audit_logger, tmp_path):
        pytest.importorskip("flask")
        from totali.quarantine_ui.app import QUARANTINE_QUEUE

        QUARANTINE_QUEUE.clear()
        try:
            gk = _mk_gatekeeper(
                audit_logger,
                jurisdiction_zones=[_ZONE_PARTIAL_2231],
            )
            _run(gk, tmp_path)
            assert "survey" in QUARANTINE_QUEUE
            assert QUARANTINE_QUEUE["survey"]["candidates"][0]["confidence"] == pytest.approx(
                0.6
            )

            from totali.quarantine_ui.app import app

            app.config["TESTING"] = True
            with app.test_client() as client:
                health = client.get("/health").get_json()
                assert health["status"] == "ok"
                assert health["queue_length"] == 1
        finally:
            QUARANTINE_QUEUE.clear()


class TestAutoAssignDisabled:
    def test_all_inferred_route_to_quarantine_when_auto_assign_off(
        self, audit_logger, tmp_path
    ):
        gk = _mk_gatekeeper(
            audit_logger,
            auto_assign_high_confidence=False,
            jurisdiction_zones=[_ZONE_FULL_CONTAIN_2231],
        )
        result, _ = _run(gk, tmp_path)
        assert result.success is False
        assert result.data.get("quarantined") is True
        assert len(audit_logger.get_events("crs_inferred")) == 0
