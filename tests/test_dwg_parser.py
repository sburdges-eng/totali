"""Tests for DWG/DXF parser production behaviors."""

from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path


def _load_parser_module():
    parser_path = (
        Path(__file__).resolve().parents[1]
        / "survey-automation-roadmap"
        / "dwg-tool-parser"
        / "scripts"
        / "parse_dwg.py"
    )
    spec = spec_from_file_location("parse_dwg_script", parser_path)
    assert spec is not None
    assert spec.loader is not None
    module = module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_build_topology_ignores_zero_length_edges():
    parser = _load_parser_module()
    entities = [
        {
            "id": "line-1",
            "type": "LINE",
            "geometry": {
                "kind": "line",
                "start": [100.0, 200.0, 0.0],
                "end": [100.0, 200.0, 0.0],
            },
        }
    ]

    topology = parser.build_topology(entities=entities, tolerance=1e-6, precision=6)

    assert topology["edge_count"] == 0
