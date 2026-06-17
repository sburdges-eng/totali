"""U2: CRS inference + jurisdiction routing — pure module tests.

`crs_inference` is deliberately dependency-light: given a (possibly absent)
declared EPSG and the data's projected XY bounds, it decides whether the CRS is
DECLARED, INFERRED (single zone match, flagged for review), AMBIGUOUS (>1 zone
-> quarantine), OUT_OF_JURISDICTION (no zone -> reject), or MISSING (no signal).

No network, no pyproj, no fabricated coordinates: jurisdiction zones are passed
in explicitly by the caller (built from config). Tests inject synthetic zones so
the suite stays fully mocked and deterministic.
"""

from totali.geodetic.crs_inference import (
    CRSInferenceStatus,
    JurisdictionZone,
    build_jurisdiction,
    infer_crs,
)


# A jurisdiction whose single zone comfortably contains the synthetic fake-LAS
# bounds (x,y in [0, 1000]); far enough margin that exact RNG bounds don't matter.
_ZONE_NORTH = JurisdictionZone(
    epsg=2231, name="NAD83 Colorado North (ftUS)", xy_min=(-1.0, -1.0), xy_max=(2000.0, 2000.0)
)
# A second zone overlapping the same envelope -> used to force AMBIGUOUS.
_ZONE_CENTRAL = JurisdictionZone(
    epsg=2232, name="NAD83 Colorado Central (ftUS)", xy_min=(-1.0, -1.0), xy_max=(2000.0, 2000.0)
)
# A disjoint zone, far away -> used to force OUT_OF_JURISDICTION.
_ZONE_FAR = JurisdictionZone(
    epsg=2233, name="NAD83 Colorado South (ftUS)", xy_min=(1.0e6, 1.0e6), xy_max=(2.0e6, 2.0e6)
)

_BOUNDS_MIN = (0.0, 0.0, 100.0)
_BOUNDS_MAX = (1000.0, 1000.0, 200.0)


class TestJurisdictionZone:
    def test_contains_inside(self):
        assert _ZONE_NORTH.contains(500.0, 500.0) is True

    def test_contains_on_edge(self):
        assert _ZONE_NORTH.contains(-1.0, 2000.0) is True

    def test_contains_outside(self):
        assert _ZONE_NORTH.contains(3000.0, 3000.0) is False


class TestBuildJurisdiction:
    def test_empty_when_unconfigured(self):
        assert build_jurisdiction({}) == []

    def test_builds_zones_from_config(self):
        cfg = {
            "jurisdiction_zones": [
                {
                    "epsg": 2231,
                    "name": "North",
                    "xy_min": [0.0, 0.0],
                    "xy_max": [1000.0, 1000.0],
                }
            ]
        }
        zones = build_jurisdiction(cfg)
        assert len(zones) == 1
        assert zones[0].epsg == 2231
        assert zones[0].name == "North"
        assert zones[0].xy_min == (0.0, 0.0)
        assert zones[0].xy_max == (1000.0, 1000.0)


class TestDeclared:
    def test_declared_in_allowlist_is_honored(self):
        result = infer_crs(
            declared_epsg=2231,
            bounds_min=_BOUNDS_MIN,
            bounds_max=_BOUNDS_MAX,
            jurisdiction=[_ZONE_NORTH],
            allowed_epsg=[2231, 2232],
        )
        assert result.status is CRSInferenceStatus.DECLARED
        assert result.is_declared is True
        assert result.inferred_epsg == 2231
        assert result.requires_quarantine is False
        assert result.is_rejected is False

    def test_declared_out_of_allowlist_is_rejected(self):
        result = infer_crs(
            declared_epsg=9999,
            bounds_min=_BOUNDS_MIN,
            bounds_max=_BOUNDS_MAX,
            jurisdiction=[_ZONE_NORTH],
            allowed_epsg=[2231, 2232],
        )
        assert result.status is CRSInferenceStatus.OUT_OF_JURISDICTION
        assert result.is_rejected is True
        assert "9999" in result.reason


class TestInferred:
    def test_single_zone_match_is_inferred_and_flagged(self):
        result = infer_crs(
            declared_epsg=None,
            bounds_min=_BOUNDS_MIN,
            bounds_max=_BOUNDS_MAX,
            jurisdiction=[_ZONE_NORTH, _ZONE_FAR],
            allowed_epsg=[2231, 2232, 2233],
        )
        assert result.status is CRSInferenceStatus.INFERRED
        assert result.inferred_epsg == 2231
        assert result.requires_review is True
        assert len(result.candidates) == 1
        assert 0.0 < result.candidates[0].confidence <= 1.0

    def test_inferred_confidence_higher_when_fully_contained(self):
        # Bounds fully inside -> higher confidence than centroid-only containment.
        fully = infer_crs(
            declared_epsg=None,
            bounds_min=_BOUNDS_MIN,
            bounds_max=_BOUNDS_MAX,
            jurisdiction=[_ZONE_NORTH],
            allowed_epsg=[2231],
        )
        partial_zone = JurisdictionZone(
            epsg=2231, name="tight", xy_min=(400.0, 400.0), xy_max=(2000.0, 2000.0)
        )
        partial = infer_crs(
            declared_epsg=None,
            bounds_min=_BOUNDS_MIN,
            bounds_max=_BOUNDS_MAX,
            jurisdiction=[partial_zone],
            allowed_epsg=[2231],
        )
        assert fully.candidates[0].confidence > partial.candidates[0].confidence


class TestAmbiguous:
    def test_two_matching_zones_route_to_quarantine(self):
        result = infer_crs(
            declared_epsg=None,
            bounds_min=_BOUNDS_MIN,
            bounds_max=_BOUNDS_MAX,
            jurisdiction=[_ZONE_NORTH, _ZONE_CENTRAL],
            allowed_epsg=[2231, 2232],
        )
        assert result.status is CRSInferenceStatus.AMBIGUOUS
        assert result.requires_quarantine is True
        assert len(result.candidates) == 2
        epsgs = {c.epsg for c in result.candidates}
        assert epsgs == {2231, 2232}


class TestOutOfJurisdiction:
    def test_bounds_outside_all_zones_rejected(self):
        result = infer_crs(
            declared_epsg=None,
            bounds_min=_BOUNDS_MIN,
            bounds_max=_BOUNDS_MAX,
            jurisdiction=[_ZONE_FAR],
            allowed_epsg=[2233],
        )
        assert result.status is CRSInferenceStatus.OUT_OF_JURISDICTION
        assert result.is_rejected is True
        assert result.requires_quarantine is False


class TestMissing:
    def test_no_declared_no_bounds_is_missing(self):
        result = infer_crs(
            declared_epsg=None,
            bounds_min=None,
            bounds_max=None,
            jurisdiction=[_ZONE_NORTH],
            allowed_epsg=[2231],
        )
        assert result.status is CRSInferenceStatus.MISSING
        # No coordinates to infer from -> operator must resolve; surface all
        # configured zones as candidates for the quarantine UI.
        assert result.requires_quarantine is True
        assert {c.epsg for c in result.candidates} == {2231}

    def test_no_declared_no_jurisdiction_is_missing_without_candidates(self):
        result = infer_crs(
            declared_epsg=None,
            bounds_min=None,
            bounds_max=None,
            jurisdiction=[],
            allowed_epsg=[2231],
        )
        assert result.status is CRSInferenceStatus.MISSING
        assert result.candidates == []
        assert result.requires_quarantine is False
