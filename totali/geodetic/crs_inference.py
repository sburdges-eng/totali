"""U2: CRS inference + jurisdiction routing.

Pure, deterministic candidate generation for the geodetic gatekeeper. Given a
(possibly absent) declared EPSG and the data's projected XY bounds, decide what
to do with a point cloud whose CRS is not already declared-and-allowed:

    DECLARED            declared EPSG is inside the jurisdiction allowlist; honor it
    INFERRED            exactly one configured jurisdiction zone contains the data;
                        low-trust, flagged for surveyor review
    AMBIGUOUS           >1 zone contains the data -> route to the quarantine UI
    OUT_OF_JURISDICTION declared-but-disallowed, or bounds outside every zone -> reject
    MISSING             no declared CRS and nothing to infer from

Design constraints (see plan U2):

- **No network, no fabricated coordinates.** Jurisdiction zone envelopes are
  supplied by the caller (built from config), never invented here. We don't have
  the design partner's State Plane zone envelope yet, so production config leaves
  ``jurisdiction_zones`` empty (a documented TODO); the *mechanism* is complete.
- **No pyproj dependency in the core.** Containment is plain interval geometry on
  projected XY, so the unit tests stay fully mocked and deterministic.
- **Advisory, not authoritative.** An INFERRED CRS is always flagged for review;
  the surveyor remains sovereign over the final CRS decision.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Optional, Sequence


class CRSInferenceStatus(str, Enum):
    DECLARED = "declared"
    INFERRED = "inferred"
    AMBIGUOUS = "ambiguous"
    OUT_OF_JURISDICTION = "out_of_jurisdiction"
    MISSING = "missing"


@dataclass(frozen=True)
class JurisdictionZone:
    """A configured State Plane zone and its projected XY applicability envelope.

    ``xy_min``/``xy_max`` are ``(easting, northing)`` in the project unit. They
    define where coordinates of this zone are expected to fall — used to infer the
    zone from coordinate magnitude when the LAS declares no CRS.
    """

    epsg: int
    name: str
    xy_min: tuple[float, float]
    xy_max: tuple[float, float]

    def contains(self, x: float, y: float) -> bool:
        return (
            self.xy_min[0] <= x <= self.xy_max[0]
            and self.xy_min[1] <= y <= self.xy_max[1]
        )


@dataclass
class CRSCandidate:
    epsg: int
    name: str
    confidence: float


@dataclass
class CRSInferenceResult:
    status: CRSInferenceStatus
    candidates: list[CRSCandidate] = field(default_factory=list)
    declared_epsg: Optional[int] = None
    reason: str = ""

    @property
    def is_declared(self) -> bool:
        return self.status is CRSInferenceStatus.DECLARED

    @property
    def is_rejected(self) -> bool:
        return self.status is CRSInferenceStatus.OUT_OF_JURISDICTION

    @property
    def requires_review(self) -> bool:
        """An inferred CRS is never authoritative — the surveyor must confirm."""
        return self.status is CRSInferenceStatus.INFERRED

    @property
    def requires_quarantine(self) -> bool:
        """Ambiguity (or a missing CRS with candidates) routes to the operator."""
        if self.status is CRSInferenceStatus.AMBIGUOUS:
            return True
        if self.status is CRSInferenceStatus.MISSING:
            return bool(self.candidates)
        return False

    @property
    def inferred_epsg(self) -> Optional[int]:
        """The single resolved EPSG for DECLARED/INFERRED; ``None`` otherwise."""
        if self.status in (CRSInferenceStatus.DECLARED, CRSInferenceStatus.INFERRED):
            return self.candidates[0].epsg if self.candidates else self.declared_epsg
        return None


def build_jurisdiction(config: dict) -> list[JurisdictionZone]:
    """Build the jurisdiction allowlist from a geodetic config section.

    Reads ``config['jurisdiction_zones']`` — a list of
    ``{epsg, name, xy_min, xy_max}`` dicts. Missing/empty -> ``[]`` (inference
    has nothing to match against and will defer to the declared/reject path).
    """
    zones: list[JurisdictionZone] = []
    for entry in config.get("jurisdiction_zones", []) or []:
        zones.append(
            JurisdictionZone(
                epsg=int(entry["epsg"]),
                name=str(entry.get("name", f"EPSG:{entry['epsg']}")),
                xy_min=(float(entry["xy_min"][0]), float(entry["xy_min"][1])),
                xy_max=(float(entry["xy_max"][0]), float(entry["xy_max"][1])),
            )
        )
    return zones


def _centroid(bounds_min: Sequence[float], bounds_max: Sequence[float]) -> tuple[float, float]:
    return (
        (bounds_min[0] + bounds_max[0]) / 2.0,
        (bounds_min[1] + bounds_max[1]) / 2.0,
    )


def _fit_confidence(
    zone: JurisdictionZone,
    bounds_min: Sequence[float],
    bounds_max: Sequence[float],
) -> float:
    """Deterministic, explainable confidence for a zone match.

    0.9 when the full XY extent sits inside the zone envelope; 0.6 when only the
    centroid is inside (extent spills past an edge). No magic — just "fully
    contained" vs "partially contained". Inference is advisory regardless.
    """
    corners = (
        (bounds_min[0], bounds_min[1]),
        (bounds_min[0], bounds_max[1]),
        (bounds_max[0], bounds_min[1]),
        (bounds_max[0], bounds_max[1]),
    )
    if all(zone.contains(cx, cy) for cx, cy in corners):
        return 0.9
    return 0.6


def infer_crs(
    *,
    declared_epsg: Optional[int],
    bounds_min: Optional[Sequence[float]],
    bounds_max: Optional[Sequence[float]],
    jurisdiction: list[JurisdictionZone],
    allowed_epsg: Sequence[int],
) -> CRSInferenceResult:
    """Resolve a CRS decision from declared metadata + projected bounds.

    See module docstring for the status taxonomy.
    """
    allowed = {e for e in allowed_epsg if e is not None}

    # 1. Declared CRS short-circuits inference.
    if declared_epsg:
        if declared_epsg in allowed:
            zone_name = next(
                (z.name for z in jurisdiction if z.epsg == declared_epsg),
                f"EPSG:{declared_epsg}",
            )
            return CRSInferenceResult(
                status=CRSInferenceStatus.DECLARED,
                candidates=[CRSCandidate(declared_epsg, zone_name, 1.0)],
                declared_epsg=declared_epsg,
                reason="CRS declared in LAS metadata and inside jurisdiction allowlist",
            )
        return CRSInferenceResult(
            status=CRSInferenceStatus.OUT_OF_JURISDICTION,
            declared_epsg=declared_epsg,
            reason=(
                f"declared EPSG:{declared_epsg} is not in the jurisdiction "
                f"allowlist {sorted(allowed)}"
            ),
        )

    # 2. No declared CRS, no coordinates -> nothing to infer from.
    if bounds_min is None or bounds_max is None:
        candidates = [
            CRSCandidate(z.epsg, z.name, 0.0) for z in jurisdiction if z.epsg in allowed
        ]
        return CRSInferenceResult(
            status=CRSInferenceStatus.MISSING,
            candidates=candidates,
            reason="no declared CRS and no coordinate bounds available to infer from",
        )

    # 3. Infer from coordinate magnitude against the configured zone envelopes.
    cx, cy = _centroid(bounds_min, bounds_max)
    matches = [z for z in jurisdiction if z.contains(cx, cy) and z.epsg in allowed]

    if len(matches) == 1:
        zone = matches[0]
        return CRSInferenceResult(
            status=CRSInferenceStatus.INFERRED,
            candidates=[
                CRSCandidate(zone.epsg, zone.name, _fit_confidence(zone, bounds_min, bounds_max))
            ],
            reason=f"coordinate bounds fall inside a single jurisdiction zone (EPSG:{zone.epsg})",
        )

    if len(matches) > 1:
        candidates = sorted(
            (
                CRSCandidate(z.epsg, z.name, _fit_confidence(z, bounds_min, bounds_max))
                for z in matches
            ),
            key=lambda c: c.confidence,
            reverse=True,
        )
        return CRSInferenceResult(
            status=CRSInferenceStatus.AMBIGUOUS,
            candidates=candidates,
            reason=(
                f"coordinate bounds match {len(matches)} jurisdiction zones "
                f"{[c.epsg for c in candidates]}; operator must disambiguate"
            ),
        )

    return CRSInferenceResult(
        status=CRSInferenceStatus.OUT_OF_JURISDICTION,
        reason="coordinate bounds fall outside every configured jurisdiction zone",
    )
