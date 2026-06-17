"""Firm-layer → TOTaLi DRAFT-layer crosswalk.

The field-code taxonomy (:mod:`totali.fieldcodes`) yields the firm's own layer
names (``TOPO``, ``CONTROL_POINT``, ``CULVERT`` …). The shield, however, only
emits layers matching the §1.3 invariant — ``TOTaLi-<DISC>-<FEAT>-DRAFT`` (or
``TOTaLi-QA-*``). This crosswalk maps firm layers into conforming DRAFT names so
coded-point geometry can be placed on legal layers.

A curated map gives clean names for the common firm layers; any other firm layer
falls back to a sanitized ``TOTaLi-<DISC>-<FEAT>-DRAFT``. Every result is checked
against the invariant pattern, so a bad mapping fails loudly rather than reaching
the shield.
"""

from __future__ import annotations

import re

# Mirrors CADShield._LAYER_NAME_RE (§1.3). Kept in sync by
# test_layer_crosswalk, which runs the shield's own validator over every output.
_DRAFT_RE = re.compile(
    r"^TOTaLi-[A-Z0-9]+(?:-[A-Z0-9_]+)+-DRAFT$|^TOTaLi-QA-[A-Z0-9_-]+$"
)

#: Points with no recognized field code land on a QA layer (not a DRAFT feature).
UNCODED_LAYER = "TOTaLi-QA-UNCODED"

#: Curated firm-layer → DRAFT names for the common (curated-taxonomy) layers.
FIRM_LAYER_CROSSWALK: dict[str, str] = {
    "TOPO": "TOTaLi-SURV-TOPO-DRAFT",
    "CONTROL_POINT": "TOTaLi-SURV-CTRL-DRAFT",
    "SURVEY_MARKER": "TOTaLi-SURV-MON-DRAFT",
    "SURVEY_MARKER_W/_LS": "TOTaLi-SURV-MONLS-DRAFT",
    "FOUND_REBAR_W/_ALUMINUM_CAP": "TOTaLi-SURV-REBARALUM-DRAFT",
    "FOUND_REBAR_W/_PLASTIC_CAP": "TOTaLi-SURV-REBARPLAS-DRAFT",
    "CREEK": "TOTaLi-SURV-CREEK-DRAFT",
    "POND": "TOTaLi-SURV-POND-DRAFT",
    "CULVERT": "TOTaLi-PLAN-CULVERT-DRAFT",
    "WATERLINE": "TOTaLi-PLAN-WATERLINE-DRAFT",
    "UTILITY": "TOTaLi-PLAN-UTILITY-DRAFT",
    "UTILITY_POLE": "TOTaLi-PLAN-UTILPOLE-DRAFT",
    "BRIDGE_ABUTMENT": "TOTaLi-PLAN-BRIDGE-DRAFT",
    "CONCRETE_BLOCK": "TOTaLi-PLAN-CONC-DRAFT",
}

_NON_ALNUM = re.compile(r"[^A-Z0-9]+")


def _sanitize_feat(firm_layer: str) -> str:
    feat = _NON_ALNUM.sub("_", firm_layer.upper()).strip("_")
    return feat or "UNCODED"


def firm_layer_to_draft(firm_layer: str, discipline: str = "SURV") -> str:
    """Map a firm layer to a conforming ``TOTaLi-<DISC>-<FEAT>-DRAFT`` name.

    Uses the curated map when present, else sanitizes ``firm_layer`` into a FEAT
    token under ``discipline``. Raises :class:`ValueError` if the result does not
    satisfy the §1.3 layer-name invariant.
    """
    name = FIRM_LAYER_CROSSWALK.get(firm_layer)
    if name is None:
        disc = _NON_ALNUM.sub("", discipline.upper()) or "SURV"
        name = f"TOTaLi-{disc}-{_sanitize_feat(firm_layer)}-DRAFT"
    if not _DRAFT_RE.match(name):
        raise ValueError(
            f"crosswalk produced non-conforming layer {name!r} for "
            f"firm layer {firm_layer!r} (violates §1.3)"
        )
    return name


def draft_layer_for(firm_layer: str | None, discipline: str = "SURV") -> str:
    """DRAFT layer for a firm layer; uncoded/empty → the QA uncoded layer."""
    if not firm_layer:
        return UNCODED_LAYER
    return firm_layer_to_draft(firm_layer, discipline)
