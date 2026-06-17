"""U2: GeodeticGatekeeper CRS-inference + quarantine routing (integration).

Exercises the gatekeeper's `run()` wiring on top of the pure `crs_inference`
module: a missing-CRS LAS is inferred from coordinate bounds against the
configured jurisdiction, with ambiguity routed to quarantine and
out-of-jurisdiction data rejected — all flask-free (the in-memory UI queue is a
best-effort side channel, the JSON artifact + audit event are authoritative).

Inference defaults OFF so every pre-existing gatekeeper test is untouched.
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
}

# Synthetic zones. The fake LAS has x,y in [0, 1000]; a wide envelope contains it.
_ZONE_WIDE_2231 = {
    "epsg": 2231,
    "name": "NAD83 Colorado North (ftUS)",
    "xy_min": [-1.0, -1.0],
    "xy_max": [2000.0, 2000.0],
}
_ZONE_WIDE_2232 = {
    "epsg": 2232,
    "name": "NAD83 Colorado Central (ftUS)",
    "xy_min": [-1.0, -1.0],
    "xy_max": [2000.0, 2000.0],
}
_ZONE_FAR_2233 = {
    "epsg": 2233,
    "name": "NAD83 Colorado South (ftUS)",
    "xy_min": [1.0e6, 1.0e6],
    "xy_max": [2.0e6, 2.0e6],
}


def _mk_gatekeeper(audit_logger, **overrides):
    cfg = dict(_BASE_CFG)
    cfg.update(overrides)
    return GeodeticGatekeeper(cfg, audit_logger)


def _make_las(tmp_path):
    """Create a real (content-irrelevant) LAS file so chain-of-custody hashing
    succeeds; the stubbed laspy.read ignores the bytes."""
    las = tmp_path / "survey.las"
    las.write_bytes(b"\x00" * 256)
    return las


def _run(gk, tmp_path):
    las = _make_las(tmp_path)
    out = tmp_path / "out"
    out.mkdir(exist_ok=True)
    ctx = PipelineContext(input_path=str(las), output_dir=out)
    return gk.run(ctx), out


class TestInferenceDisabledPreservesBehavior:
    def test_missing_crs_still_rejected_when_inference_off(self, audit_logger, tmp_path):
        gk = _mk_gatekeeper(audit_logger)  # crs_inference_enabled defaults False
        result, _ = _run(gk, tmp_path)
        assert result.success is False
        assert "CRS validation failed" in result.message


class TestSingleZoneInference:
    def test_single_zone_match_infers_and_proceeds(self, audit_logger, tmp_path):
        gk = _mk_gatekeeper(
            audit_logger,
            crs_inference_enabled=True,
            jurisdiction_zones=[_ZONE_WIDE_2231, _ZONE_FAR_2233],
        )
        result, _ = _run(gk, tmp_path)
        assert result.success is True
        assert result.data["crs"].epsg_code == 2231

        inferred = audit_logger.get_events("crs_inferred")
        assert len(inferred) == 1
        assert inferred[0]["data"]["epsg"] == 2231
        assert inferred[0]["data"]["requires_review"] is True


class TestAmbiguousQuarantine:
    def test_two_zones_route_to_quarantine(self, audit_logger, tmp_path):
        gk = _mk_gatekeeper(
            audit_logger,
            crs_inference_enabled=True,
            jurisdiction_zones=[_ZONE_WIDE_2231, _ZONE_WIDE_2232],
        )
        result, out = _run(gk, tmp_path)

        assert result.success is False
        assert result.data.get("quarantined") is True

        artifact = out / "survey_crs_quarantine.json"
        assert artifact.exists()
        payload = json.loads(artifact.read_text())
        assert {c["epsg"] for c in payload["candidates"]} == {2231, 2232}
        assert payload["status"] == "ambiguous"

        events = audit_logger.get_events("crs_quarantined")
        assert len(events) == 1
        assert events[0]["data"]["item_id"] == "survey"


class TestOutOfJurisdiction:
    def test_bounds_outside_zones_rejected(self, audit_logger, tmp_path):
        gk = _mk_gatekeeper(
            audit_logger,
            crs_inference_enabled=True,
            jurisdiction_zones=[_ZONE_FAR_2233],
        )
        result, _ = _run(gk, tmp_path)
        assert result.success is False
        assert result.data.get("rejected") is True
        assert len(audit_logger.get_events("crs_out_of_jurisdiction")) == 1


class TestOperatorResolutionHonored:
    def test_existing_confirm_resolution_is_honored(self, audit_logger, tmp_path):
        # Operator already resolved this item via the quarantine UI; the gatekeeper
        # must honor the written resolution on the next run instead of re-inferring.
        out = tmp_path / "out"
        out.mkdir()
        (out / "survey_crs_resolution.json").write_text(
            json.dumps(
                {"item_id": "survey", "action": "confirm", "epsg": 2232, "operator": "alice"}
            )
        )
        gk = _mk_gatekeeper(
            audit_logger,
            crs_inference_enabled=True,
            jurisdiction_zones=[_ZONE_WIDE_2231, _ZONE_WIDE_2232],  # would be ambiguous
        )
        ctx = PipelineContext(input_path=str(_make_las(tmp_path)), output_dir=out)
        result = gk.run(ctx)
        assert result.success is True
        assert result.data["crs"].epsg_code == 2232


class TestQuarantineQueueWiring:
    def test_ambiguous_enqueues_when_ui_available(self, audit_logger, tmp_path):
        pytest.importorskip("flask")
        from totali.quarantine_ui.app import QUARANTINE_QUEUE

        QUARANTINE_QUEUE.clear()
        try:
            gk = _mk_gatekeeper(
                audit_logger,
                crs_inference_enabled=True,
                jurisdiction_zones=[_ZONE_WIDE_2231, _ZONE_WIDE_2232],
            )
            _run(gk, tmp_path)
            assert "survey" in QUARANTINE_QUEUE
            assert {c["epsg"] for c in QUARANTINE_QUEUE["survey"]["candidates"]} == {2231, 2232}
        finally:
            QUARANTINE_QUEUE.clear()
