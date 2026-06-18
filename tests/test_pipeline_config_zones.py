"""Production config/pipeline.yaml jurisdiction-zone contract.

Provisional demo-derived jurisdiction_zones were promoted into the production
config so CRS inference *can* be exercised once the partner confirms their
operating envelope. This test pins the safety contract:
  * the zones are present and well-formed, and
  * crs_inference_enabled stays FALSE — provisional (not partner-confirmed)
    zones must NOT silently enable inference in production ingestion.
Flip crs_inference_enabled only when real partner zones replace the provisional
ones (see Docs/MEETING_CAPTURE_SHEET_2026-06-18.md Part D).
"""

from pathlib import Path

import yaml

_CFG = Path(__file__).resolve().parents[1] / "config" / "pipeline.yaml"


def _geodetic() -> dict:
    with open(_CFG) as f:
        return yaml.safe_load(f)["geodetic"]


def test_jurisdiction_zones_present_and_well_formed():
    g = _geodetic()
    zones = g.get("jurisdiction_zones")
    assert zones, "production jurisdiction_zones should be populated (provisional demo zones)"
    allowed = {int(c.split(":")[1]) for c in g["allowed_crs"]}
    for z in zones:
        assert {"epsg", "name", "xy_min", "xy_max"} <= set(z), f"zone missing keys: {z}"
        assert z["epsg"] in allowed, f"zone epsg {z['epsg']} not in allowed_crs"
        assert len(z["xy_min"]) == 2 and len(z["xy_max"]) == 2
        assert z["xy_min"][0] < z["xy_max"][0] and z["xy_min"][1] < z["xy_max"][1], (
            f"zone envelope must have xy_min < xy_max: {z}"
        )


def test_provisional_zones_keep_inference_disabled():
    # Safety invariant: provisional (not partner-confirmed) zones must not
    # silently turn on CRS inference in the production config.
    assert _geodetic().get("crs_inference_enabled") is False


def test_provisional_zones_are_labeled():
    # Each promoted zone must carry the PROVISIONAL marker so it is never
    # mistaken for a partner-confirmed operating envelope.
    for z in _geodetic()["jurisdiction_zones"]:
        assert "PROVISIONAL" in z["name"], f"zone not marked provisional: {z['name']}"
