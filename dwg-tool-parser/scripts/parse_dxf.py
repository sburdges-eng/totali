"""Minimal ezdxf-backed DXF parser emitting schema-valid JSON.

Implements DP-3 (see dwg-tool-parser/AGENTIC.md): the smallest useful parser
that turns a DXF file into the canonical JSON shape defined by
``dwg-tool-parser/schema/parser_output.schema.json`` (DP-2).

DWG ingest is out of scope here — that's DP-1/DP-4 (LibreDWG). This module
is DXF-only and does no survey-tag mapping yet (``survey_tags`` is always
``[]``; DP-5 extends).

CLI usage::

    python -m scripts.parse_dxf <dxf_path> [--output out.json]

Exits 0 on a clean parse, 3 on any ezdxf-level error (file not found,
corrupt DXF, unreadable structure).
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

SCHEMA_VERSION = "1.0.0"
_EXCLUDED_BLOCK_NAMES = {"*Model_Space", "*Paper_Space"}
# ezdxf stores model/paper space with alternate case in some DXF versions.
_EXCLUDED_BLOCK_NAMES_LOWER = {name.lower() for name in _EXCLUDED_BLOCK_NAMES}


# ---------------------------------------------------------------------------
# Entity-property extraction
# ---------------------------------------------------------------------------

def _point_to_list(point: Any) -> list[float]:
    """Normalize an ezdxf point-like (Vec3/Vec2/tuple) to a plain list."""
    if point is None:
        return []
    # ezdxf Vec3 exposes x/y/z; fall back to iteration for tuples.
    x = getattr(point, "x", None)
    y = getattr(point, "y", None)
    z = getattr(point, "z", None)
    if x is not None and y is not None:
        return [float(x), float(y), float(z) if z is not None else 0.0]
    try:
        seq = list(point)
    except TypeError:
        return []
    if len(seq) >= 3:
        return [float(seq[0]), float(seq[1]), float(seq[2])]
    if len(seq) == 2:
        return [float(seq[0]), float(seq[1]), 0.0]
    return []


def _extract_entity_properties(entity: Any) -> dict[str, Any]:
    """Pull entity-type-specific properties into a free-form dict.

    Covers the entity kinds the DP-3 fixture uses (LINE, LWPOLYLINE, INSERT)
    plus a small set of common neighbors (POLYLINE, CIRCLE, ARC, POINT,
    TEXT). Unknown entity kinds return an empty dict — the contract allows
    free-form ``properties`` per the schema, so this is valid.
    """
    dxftype = entity.dxftype()
    props: dict[str, Any] = {}

    if dxftype == "LINE":
        props["start"] = _point_to_list(entity.dxf.start)
        props["end"] = _point_to_list(entity.dxf.end)
    elif dxftype == "LWPOLYLINE":
        # get_points("xy") gives 2D vertices; polyline is closed per flag 70 bit 1.
        vertices = [[float(x), float(y)] for x, y, *_ in entity.get_points("xy")]
        props["vertices"] = vertices
        props["closed"] = bool(entity.closed)
    elif dxftype == "POLYLINE":
        vertices = [
            _point_to_list(v.dxf.location) for v in entity.vertices
        ]
        props["vertices"] = vertices
        props["closed"] = bool(entity.is_closed)
    elif dxftype == "CIRCLE":
        props["center"] = _point_to_list(entity.dxf.center)
        props["radius"] = float(entity.dxf.radius)
    elif dxftype == "ARC":
        props["center"] = _point_to_list(entity.dxf.center)
        props["radius"] = float(entity.dxf.radius)
        props["start_angle"] = float(entity.dxf.start_angle)
        props["end_angle"] = float(entity.dxf.end_angle)
    elif dxftype == "POINT":
        props["location"] = _point_to_list(entity.dxf.location)
    elif dxftype == "TEXT":
        props["insert"] = _point_to_list(entity.dxf.insert)
        props["text"] = str(entity.dxf.text)
    elif dxftype == "INSERT":
        props["block_name"] = str(entity.dxf.name)
        props["insertion_point"] = _point_to_list(entity.dxf.insert)
        props["rotation_deg"] = float(entity.dxf.rotation)
        props["scale"] = [
            float(entity.dxf.xscale),
            float(entity.dxf.yscale),
            float(entity.dxf.zscale),
        ]
    # Unknown entity types fall through with empty props — schema allows it.
    return props


# ---------------------------------------------------------------------------
# Layer / block counting
# ---------------------------------------------------------------------------

def _iter_top_level_layouts(doc: Any) -> list[Any]:
    """Return modelspace and, if present, paperspace layouts.

    ``doc.paperspace()`` raises ``DXFStructureError`` (via
    ``active_layout()``) when no active paperspace exists, or ``KeyError``
    when a named paperspace is not found.  Both cases are common in minimal
    or R12 DXF exports.
    """
    import ezdxf  # type: ignore[import-untyped]

    layouts: list[Any] = [doc.modelspace()]
    try:
        layouts.append(doc.paperspace())
    except (KeyError, ezdxf.DXFStructureError):
        pass
    return layouts


def _count_entities_per_layer(doc: Any) -> dict[str, int]:
    """Count every entity in modelspace + paperspace by its layer name.

    This intentionally does not include entities nested inside block
    definitions — those are counted under the block, not the referencing
    layer.
    """
    counts: dict[str, int] = {}
    for layout in _iter_top_level_layouts(doc):
        for entity in layout:
            name = entity.dxf.layer
            counts[name] = counts.get(name, 0) + 1
    return counts


def _collect_layers(doc: Any) -> list[dict[str, Any]]:
    counts = _count_entities_per_layer(doc)
    layers: list[dict[str, Any]] = []
    for layer in doc.layers:
        name = layer.dxf.name
        layers.append({"name": name, "entity_count": int(counts.get(name, 0))})
    return layers


def _collect_blocks(doc: Any) -> list[dict[str, Any]]:
    blocks: list[dict[str, Any]] = []
    for block in doc.blocks:
        name = block.name
        if name in _EXCLUDED_BLOCK_NAMES or name.lower() in _EXCLUDED_BLOCK_NAMES_LOWER:
            continue
        entity_count = sum(1 for _ in block)
        blocks.append({"name": name, "entity_count": int(entity_count)})
    return blocks


def _collect_entities(doc: Any) -> list[dict[str, Any]]:
    """Top-level entities (modelspace + paperspace, not block contents)."""
    entities: list[dict[str, Any]] = []
    for layout in _iter_top_level_layouts(doc):
        for entity in layout:
            entities.append(
                {
                    "id": str(entity.dxf.handle),
                    "type": entity.dxftype(),
                    "layer": str(entity.dxf.layer),
                    "properties": _extract_entity_properties(entity),
                }
            )
    return entities


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def parse_dxf(path: Path | str) -> dict[str, Any]:
    """Parse a DXF file into the canonical parser-output dict.

    Raises ``FileNotFoundError`` if ``path`` does not exist, and propagates
    ``ezdxf.DXFStructureError`` on malformed files. The CLI wrapper maps
    both classes of failure to exit code 3.
    """
    # ezdxf is imported lazily so that ``import scripts.parse_dxf`` does
    # not fail on environments without ezdxf — the CLI/parse_dxf entry
    # points themselves require it, but importing the module for its
    # helpers should not.
    import ezdxf  # type: ignore[import-untyped]

    path_obj = Path(path)
    if not path_obj.exists():
        raise FileNotFoundError(f"DXF file not found: {path_obj}")

    doc = ezdxf.readfile(str(path_obj))

    return {
        "schemaVersion": SCHEMA_VERSION,
        "file": str(path),
        "format": "dxf",
        "version": doc.dxfversion,
        "layers": _collect_layers(doc),
        "entities": _collect_entities(doc),
        "blocks": _collect_blocks(doc),
        "survey_tags": [],
        "errors": [],
    }


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def _build_argparser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="parse_dxf",
        description=(
            "Minimal ezdxf-backed DXF parser. Emits schema-valid JSON to "
            "stdout or to --output. See dwg-tool-parser/AGENTIC.md DP-3."
        ),
    )
    parser.add_argument("dxf_path", type=str, help="Path to a DXF file.")
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help="Optional output JSON path. Default: stdout.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    ns = _build_argparser().parse_args(argv)
    try:
        report = parse_dxf(ns.dxf_path)
    except FileNotFoundError as exc:
        print(f"parse_dxf: {exc}", file=sys.stderr)
        return 3
    except Exception as exc:  # noqa: BLE001 — ezdxf raises many shapes
        # ezdxf.DXFStructureError, encoding errors, etc.
        print(f"parse_dxf: failed to parse DXF: {exc}", file=sys.stderr)
        return 3

    text = json.dumps(report, indent=2, sort_keys=True)
    if ns.output:
        out_path = Path(ns.output)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(text, encoding="utf-8")
    else:
        print(text)
    return 0


if __name__ == "__main__":
    sys.exit(main())
