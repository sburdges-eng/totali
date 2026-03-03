"""Tests for DWG/DXF parser production behaviors.

Covers:
- Utility functions (safe_float, safe_int, round_number, normalize_point, etc.)
- Geometry extraction and measurement (topology primitives, length, area)
- Layer classification and entity inference
- ASCII DXF parsing pipeline
- Topology graph construction (multi-component, loops, arcs, junctions)
- Civil survey summary and domain confidence scoring
- Converter command building and input validation
"""

from __future__ import annotations

import json
import math
import textwrap
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest


# ---------------------------------------------------------------------------
# Module loader
# ---------------------------------------------------------------------------

def _load_parser_module():
    parser_path = (
        Path(__file__).resolve().parents[1]
        / "groundtruthos-data"
        / "survey-automation"
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


@pytest.fixture(scope="module")
def parser():
    return _load_parser_module()


# ===================================================================
# 1. Utility functions
# ===================================================================

class TestSafeFloat:
    def test_valid_int(self, parser):
        assert parser.safe_float(42) == 42.0

    def test_valid_str(self, parser):
        assert parser.safe_float("3.14") == pytest.approx(3.14)

    def test_none_returns_none(self, parser):
        assert parser.safe_float(None) is None

    def test_garbage_returns_none(self, parser):
        assert parser.safe_float("abc") is None

    def test_empty_string_returns_none(self, parser):
        assert parser.safe_float("") is None


class TestSafeInt:
    def test_valid_int(self, parser):
        assert parser.safe_int(42) == 42

    def test_valid_str(self, parser):
        assert parser.safe_int("7") == 7

    def test_float_truncates(self, parser):
        assert parser.safe_int(3.9) == 3

    def test_none_returns_none(self, parser):
        assert parser.safe_int(None) is None

    def test_garbage_returns_none(self, parser):
        assert parser.safe_int("xyz") is None


class TestRoundNumber:
    def test_basic_rounding(self, parser):
        assert parser.round_number(1.23456789, 4) == pytest.approx(1.2346)

    def test_near_zero_snaps(self, parser):
        assert parser.round_number(0.0000001, 6) == 0.0

    def test_string_input(self, parser):
        assert parser.round_number("9.87", 2) == pytest.approx(9.87)

    def test_none_returns_none(self, parser):
        assert parser.round_number(None, 3) is None

    def test_garbage_returns_none(self, parser):
        assert parser.round_number("xyz", 3) is None


class TestNormalizePoint:
    def test_3d_list(self, parser):
        result = parser.normalize_point([1.0, 2.0, 3.0], 6)
        assert result == [1.0, 2.0, 3.0]

    def test_2d_list_gets_z_zero(self, parser):
        result = parser.normalize_point([1.0, 2.0], 6)
        assert result == [1.0, 2.0, 0.0]

    def test_tuple_input(self, parser):
        result = parser.normalize_point((5.0, 6.0, 7.0), 6)
        assert result == [5.0, 6.0, 7.0]

    def test_object_with_xyz(self, parser):
        class FakePoint:
            x, y, z = 10.0, 20.0, 30.0
        result = parser.normalize_point(FakePoint(), 6)
        assert result == [10.0, 20.0, 30.0]

    def test_object_with_xy_only(self, parser):
        class FakePoint2D:
            x, y = 10.0, 20.0
        result = parser.normalize_point(FakePoint2D(), 6)
        assert result == [10.0, 20.0, 0.0]

    def test_none_returns_none(self, parser):
        assert parser.normalize_point(None, 6) is None

    def test_single_element_returns_none(self, parser):
        assert parser.normalize_point([1.0], 6) is None

    def test_non_numeric_returns_none(self, parser):
        assert parser.normalize_point(["a", "b", "c"], 6) is None

    def test_precision_applied(self, parser):
        result = parser.normalize_point([1.123456789, 2.987654321, 0.0], 3)
        assert result == [pytest.approx(1.123), pytest.approx(2.988), 0.0]


class TestNormalizeToken:
    def test_strips_special_chars(self, parser):
        assert parser.normalize_token("Hello-World_123") == "helloworld123"

    def test_lowercases(self, parser):
        assert parser.normalize_token("ABC") == "abc"


class TestSplitTokens:
    def test_basic_split(self, parser):
        tokens = parser.split_tokens("parcel-boundary")
        assert "parcel" in tokens
        assert "boundary" in tokens
        assert "parcelboundary" in tokens

    def test_empty_returns_empty(self, parser):
        assert parser.split_tokens("") == set()

    def test_single_word(self, parser):
        tokens = parser.split_tokens("lot")
        assert "lot" in tokens

    def test_compound_join(self, parser):
        tokens = parser.split_tokens("Right Of Way")
        assert "right_of_way" in tokens
        assert "rightofway" in tokens


class TestCollectContextTokens:
    def test_collects_from_multiple(self, parser):
        tokens = parser.collect_context_tokens(["road-center", "boundary"])
        assert "road" in tokens
        assert "center" in tokens
        assert "boundary" in tokens


class TestIsPoint:
    def test_valid_3d(self, parser):
        assert parser.is_point([1.0, 2.0, 3.0]) is True

    def test_2d_too_short(self, parser):
        assert parser.is_point([1.0, 2.0]) is False

    def test_non_numeric(self, parser):
        assert parser.is_point(["a", "b", "c"]) is False

    def test_not_list(self, parser):
        assert parser.is_point("abc") is False


class TestDistance3d:
    def test_axis_aligned(self, parser):
        assert parser.distance_3d([0, 0, 0], [3, 4, 0]) == pytest.approx(5.0)

    def test_same_point(self, parser):
        assert parser.distance_3d([1, 1, 1], [1, 1, 1]) == 0.0

    def test_3d_diagonal(self, parser):
        assert parser.distance_3d([0, 0, 0], [1, 1, 1]) == pytest.approx(math.sqrt(3))


class TestPolylineLength:
    def test_two_points(self, parser):
        assert parser.polyline_length([[0, 0, 0], [3, 4, 0]], False) == pytest.approx(5.0)

    def test_closed_adds_closing_segment(self, parser):
        verts = [[0, 0, 0], [3, 0, 0], [3, 4, 0]]
        open_len = parser.polyline_length(verts, False)
        closed_len = parser.polyline_length(verts, True)
        assert closed_len > open_len
        assert closed_len == pytest.approx(3.0 + 4.0 + 5.0)

    def test_single_point(self, parser):
        assert parser.polyline_length([[0, 0, 0]], False) == 0.0


class TestPolygonAreaXy:
    def test_unit_square(self, parser):
        square = [[0, 0, 0], [1, 0, 0], [1, 1, 0], [0, 1, 0]]
        assert parser.polygon_area_xy(square) == pytest.approx(1.0)

    def test_triangle(self, parser):
        tri = [[0, 0, 0], [4, 0, 0], [0, 3, 0]]
        assert parser.polygon_area_xy(tri) == pytest.approx(6.0)

    def test_too_few_points(self, parser):
        assert parser.polygon_area_xy([[0, 0, 0], [1, 1, 0]]) == 0.0


class TestExtractSpotElevationValue:
    def test_labeled_el(self, parser):
        assert parser.extract_spot_elevation_value("EL=123.45") == pytest.approx(123.45)

    def test_labeled_elev(self, parser):
        assert parser.extract_spot_elevation_value("ELEV 567.8") == pytest.approx(567.8)

    def test_plain_numeric(self, parser):
        assert parser.extract_spot_elevation_value("42.5") == pytest.approx(42.5)

    def test_negative_numeric(self, parser):
        assert parser.extract_spot_elevation_value("-10.2") == pytest.approx(-10.2)

    def test_non_numeric_returns_none(self, parser):
        assert parser.extract_spot_elevation_value("parcel-line") is None

    def test_empty_returns_none(self, parser):
        assert parser.extract_spot_elevation_value("") is None


class TestArcSweepDegrees:
    def test_90_degrees(self, parser):
        assert parser.arc_sweep_degrees(0, 90) == pytest.approx(90.0)

    def test_wrap_around(self, parser):
        assert parser.arc_sweep_degrees(350, 10) == pytest.approx(20.0)

    def test_full_circle(self, parser):
        assert parser.arc_sweep_degrees(0, 360) == pytest.approx(360.0)


# ===================================================================
# 2. Geometry extraction and measurement
# ===================================================================

class TestExtractTopologyPrimitives:
    def test_line_produces_edge(self, parser):
        entity = {
            "geometry": {"kind": "line", "start": [0, 0, 0], "end": [10, 0, 0]},
        }
        edges, loops = parser.extract_topology_primitives(entity, 6)
        assert len(edges) == 1
        assert edges[0]["kind"] == "segment"
        assert len(loops) == 0

    def test_closed_polyline_produces_edges_and_loop(self, parser):
        entity = {
            "geometry": {
                "kind": "polyline",
                "vertices": [[0, 0, 0], [10, 0, 0], [10, 10, 0]],
                "closed": True,
            },
        }
        edges, loops = parser.extract_topology_primitives(entity, 6)
        assert len(edges) == 3
        assert len(loops) == 1
        assert loops[0]["kind"] == "polyline"

    def test_open_polyline_no_loop(self, parser):
        entity = {
            "geometry": {
                "kind": "polyline",
                "vertices": [[0, 0, 0], [10, 0, 0], [20, 0, 0]],
                "closed": False,
            },
        }
        edges, loops = parser.extract_topology_primitives(entity, 6)
        assert len(edges) == 2
        assert len(loops) == 0

    def test_circle_produces_loop(self, parser):
        entity = {
            "geometry": {"kind": "circle", "center": [5, 5, 0], "radius": 10.0},
        }
        edges, loops = parser.extract_topology_primitives(entity, 6)
        assert len(edges) == 0
        assert len(loops) == 1
        assert loops[0]["kind"] == "circle"

    def test_arc_produces_edge(self, parser):
        entity = {
            "geometry": {
                "kind": "arc",
                "center": [0, 0, 0],
                "radius": 5.0,
                "start_angle": 0.0,
                "end_angle": 90.0,
                "start": [5, 0, 0],
                "end": [0, 5, 0],
            },
        }
        edges, loops = parser.extract_topology_primitives(entity, 6)
        assert len(edges) == 1
        assert edges[0]["kind"] == "arc"

    def test_spline_produces_edge(self, parser):
        entity = {
            "geometry": {
                "kind": "spline",
                "control_points": [[0, 0, 0], [5, 5, 0], [10, 0, 0]],
                "start": [0, 0, 0],
                "end": [10, 0, 0],
            },
        }
        edges, loops = parser.extract_topology_primitives(entity, 6)
        assert len(edges) == 1
        assert edges[0]["kind"] == "spline"

    def test_no_geometry_returns_empty(self, parser):
        edges, loops = parser.extract_topology_primitives({"id": "x"}, 6)
        assert edges == []
        assert loops == []

    def test_text_entity_returns_empty(self, parser):
        entity = {
            "geometry": {"kind": "text", "point": [0, 0, 0], "text": "hello"},
        }
        edges, loops = parser.extract_topology_primitives(entity, 6)
        assert edges == []
        assert loops == []


class TestEstimateGeometryLength:
    def test_line_length(self, parser):
        geom = {"kind": "line", "start": [0, 0, 0], "end": [3, 4, 0]}
        assert parser.estimate_geometry_length(geom, 6) == pytest.approx(5.0)

    def test_polyline_length(self, parser):
        geom = {
            "kind": "polyline",
            "vertices": [[0, 0, 0], [10, 0, 0], [10, 10, 0]],
            "closed": False,
        }
        assert parser.estimate_geometry_length(geom, 6) == pytest.approx(20.0)

    def test_circle_circumference(self, parser):
        geom = {"kind": "circle", "center": [0, 0, 0], "radius": 1.0}
        assert parser.estimate_geometry_length(geom, 6) == pytest.approx(2 * math.pi)

    def test_arc_length(self, parser):
        geom = {
            "kind": "arc",
            "center": [0, 0, 0],
            "radius": 10.0,
            "start_angle": 0.0,
            "end_angle": 90.0,
        }
        expected = 10.0 * math.radians(90)
        assert parser.estimate_geometry_length(geom, 6) == pytest.approx(expected)

    def test_unknown_kind_returns_none(self, parser):
        assert parser.estimate_geometry_length({"kind": "unknown"}, 6) is None


class TestEstimateGeometryArea:
    def test_closed_polyline_area(self, parser):
        geom = {
            "kind": "polyline",
            "vertices": [[0, 0, 0], [10, 0, 0], [10, 10, 0], [0, 10, 0]],
            "closed": True,
        }
        assert parser.estimate_geometry_area(geom, 6) == pytest.approx(100.0)

    def test_open_polyline_returns_none(self, parser):
        geom = {
            "kind": "polyline",
            "vertices": [[0, 0, 0], [10, 0, 0]],
            "closed": False,
        }
        assert parser.estimate_geometry_area(geom, 6) is None

    def test_circle_area(self, parser):
        geom = {"kind": "circle", "center": [0, 0, 0], "radius": 5.0}
        assert parser.estimate_geometry_area(geom, 6) == pytest.approx(math.pi * 25)

    def test_line_returns_none(self, parser):
        geom = {"kind": "line", "start": [0, 0, 0], "end": [10, 0, 0]}
        assert parser.estimate_geometry_area(geom, 6) is None


class TestGeometryPoints:
    def test_line_points(self, parser):
        geom = {"kind": "line", "start": [1, 2, 3], "end": [4, 5, 6]}
        pts = parser.geometry_points(geom, 6)
        assert len(pts) == 2

    def test_polyline_points(self, parser):
        geom = {"kind": "polyline", "vertices": [[0, 0, 0], [1, 1, 0], [2, 2, 0]]}
        pts = parser.geometry_points(geom, 6)
        assert len(pts) == 3

    def test_point_entity(self, parser):
        geom = {"kind": "point", "point": [10, 20, 30]}
        pts = parser.geometry_points(geom, 6)
        assert len(pts) == 1

    def test_text_entity_with_point(self, parser):
        geom = {"kind": "text", "point": [1, 2, 3], "text": "EL=100"}
        pts = parser.geometry_points(geom, 6)
        assert len(pts) == 1

    def test_insert_entity(self, parser):
        geom = {"kind": "insert", "insertion_point": [5, 5, 0]}
        pts = parser.geometry_points(geom, 6)
        assert len(pts) == 1


# ===================================================================
# 3. Layer classification
# ===================================================================

class TestLayerMatchesKeyword:
    def test_exact_match(self, parser):
        assert parser.layer_matches_keyword("PARCEL", "parcel") is True

    def test_compound_match(self, parser):
        assert parser.layer_matches_keyword("PARCEL-BOUNDARY", "boundary") is True

    def test_short_keyword_word_boundary(self, parser):
        assert parser.layer_matches_keyword("BM-CTRL", "bm") is True

    def test_short_keyword_no_false_positive(self, parser):
        assert parser.layer_matches_keyword("TIMBER", "bm") is False

    def test_empty_layer(self, parser):
        assert parser.layer_matches_keyword("", "parcel") is False


class TestClassifyLayer:
    def test_parcel_layer(self, parser):
        classes = parser.classify_layer("LOT-BOUNDARY")
        assert "parcel_boundary" in classes

    def test_centerline_layer(self, parser):
        classes = parser.classify_layer("ROAD-CENTERLINE")
        assert "centerline" in classes

    def test_contour_layer(self, parser):
        classes = parser.classify_layer("TOPO-CONTOUR")
        assert "contour" in classes

    def test_utility_layer(self, parser):
        classes = parser.classify_layer("WATER-MAIN")
        assert "utility" in classes

    def test_control_layer(self, parser):
        classes = parser.classify_layer("GPS-CONTROL")
        assert "control_point" in classes

    def test_construction_layer(self, parser):
        classes = parser.classify_layer("ASBUILT-ROADWAY")
        assert "construction" in classes

    def test_multi_classification(self, parser):
        classes = parser.classify_layer("STORM-DRAIN")
        assert "utility" in classes or "drainage" in classes

    def test_none_layer(self, parser):
        assert parser.classify_layer(None) == set()

    def test_unknown_layer(self, parser):
        assert parser.classify_layer("FOOBAR-BAZZLE") == set()


class TestInferEntityClasses:
    def test_with_layer(self, parser):
        entity = {"layer": "PARCEL-BOUNDARY"}
        classes = parser.infer_entity_classes(entity)
        assert "parcel_boundary" in classes

    def test_no_layer(self, parser):
        assert parser.infer_entity_classes({"id": "1"}) == set()


# ===================================================================
# 4. ASCII DXF parsing pipeline
# ===================================================================

class TestPairsFromDxfText:
    def test_basic_pairs(self, parser):
        text = "0\nSECTION\n2\nHEADER\n0\nENDSEC\n"
        pairs = parser.pairs_from_dxf_text(text)
        assert ("0", "SECTION") in pairs
        assert ("2", "HEADER") in pairs
        assert ("0", "ENDSEC") in pairs

    def test_empty_text(self, parser):
        assert parser.pairs_from_dxf_text("") == []


class TestExtractHeaderVar:
    def test_finds_insunits(self, parser):
        pairs = [("9", "$INSUNITS"), ("70", "6")]
        result = parser.extract_header_var(pairs, "$INSUNITS")
        assert result == "6"

    def test_finds_acadver(self, parser):
        pairs = [("9", "$ACADVER"), ("1", "AC1032")]
        result = parser.extract_header_var(pairs, "$ACADVER")
        assert result == "AC1032"

    def test_missing_var(self, parser):
        pairs = [("9", "$ACADVER"), ("1", "AC1032")]
        assert parser.extract_header_var(pairs, "$INSUNITS") is None


class TestNormalizeAsciiEntity:
    def test_line_entity(self, parser):
        raw = {
            "type": "LINE",
            "attrs": [
                ("5", "ABC"),
                ("8", "BOUNDARY"),
                ("10", "100.0"),
                ("20", "200.0"),
                ("30", "0.0"),
                ("11", "300.0"),
                ("21", "400.0"),
                ("31", "0.0"),
            ],
        }
        entity = parser.normalize_ascii_entity(raw, 0, 6)
        assert entity["id"] == "ABC"
        assert entity["type"] == "LINE"
        assert entity["layer"] == "BOUNDARY"
        assert entity["geometry"]["kind"] == "line"
        assert entity["geometry"]["start"] == [100.0, 200.0, 0.0]
        assert entity["geometry"]["end"] == [300.0, 400.0, 0.0]

    def test_point_entity(self, parser):
        raw = {
            "type": "POINT",
            "attrs": [
                ("5", "P1"),
                ("8", "CONTROL"),
                ("10", "50.0"),
                ("20", "60.0"),
                ("30", "70.0"),
            ],
        }
        entity = parser.normalize_ascii_entity(raw, 0, 6)
        assert entity["geometry"]["kind"] == "point"
        assert entity["geometry"]["point"] == [50.0, 60.0, 70.0]

    def test_circle_entity(self, parser):
        raw = {
            "type": "CIRCLE",
            "attrs": [
                ("5", "C1"),
                ("10", "0.0"),
                ("20", "0.0"),
                ("30", "0.0"),
                ("40", "25.5"),
            ],
        }
        entity = parser.normalize_ascii_entity(raw, 0, 6)
        assert entity["geometry"]["kind"] == "circle"
        assert entity["geometry"]["radius"] == pytest.approx(25.5)

    def test_text_entity(self, parser):
        raw = {
            "type": "TEXT",
            "attrs": [
                ("5", "T1"),
                ("8", "SPOT-ELEV"),
                ("10", "10.0"),
                ("20", "20.0"),
                ("30", "0.0"),
                ("1", "EL=123.45"),
            ],
        }
        entity = parser.normalize_ascii_entity(raw, 0, 6)
        assert entity["geometry"]["kind"] == "text"
        assert entity["geometry"]["text"] == "EL=123.45"

    def test_lwpolyline_entity(self, parser):
        raw = {
            "type": "LWPOLYLINE",
            "attrs": [
                ("5", "LP1"),
                ("70", "1"),
                ("38", "100.0"),
                ("10", "0.0"),
                ("20", "0.0"),
                ("10", "10.0"),
                ("20", "0.0"),
                ("10", "10.0"),
                ("20", "10.0"),
            ],
        }
        entity = parser.normalize_ascii_entity(raw, 0, 6)
        assert entity["geometry"]["kind"] == "polyline"
        assert entity["geometry"]["closed"] is True
        assert len(entity["geometry"]["vertices"]) == 3

    def test_auto_generated_id(self, parser):
        raw = {"type": "POINT", "attrs": [("10", "0"), ("20", "0"), ("30", "0")]}
        entity = parser.normalize_ascii_entity(raw, 5, 6)
        assert entity["id"] == "entity-6"

    def test_entity_without_geometry(self, parser):
        raw = {"type": "UNKNOWN_TYPE", "attrs": [("8", "MISC")]}
        entity = parser.normalize_ascii_entity(raw, 0, 6)
        assert "geometry" not in entity


class TestParseAsciiDxf:
    def test_minimal_dxf(self, parser, tmp_path):
        dxf_content = textwrap.dedent("""\
            0
            SECTION
            2
            HEADER
            9
            $ACADVER
            1
            AC1032
            9
            $INSUNITS
            70
            6
            0
            ENDSEC
            0
            SECTION
            2
            ENTITIES
            0
            LINE
            5
            L1
            8
            BOUNDARY
            10
            0.0
            20
            0.0
            30
            0.0
            11
            100.0
            21
            0.0
            31
            0.0
            0
            ENDSEC
            0
            EOF
        """)
        dxf_file = tmp_path / "test.dxf"
        dxf_file.write_text(dxf_content, encoding="utf-8")

        result = parser.parse_ascii_dxf(dxf_file, sample_limit=25, precision=6)

        assert result["backend"] == "ascii-fallback"
        assert result["summary"]["dxf_version"] == "AC1032"
        assert result["summary"]["insunits"] == 6
        assert len(result["entities"]) == 1
        assert result["entities"][0]["type"] == "LINE"
        assert result["entities"][0]["geometry"]["kind"] == "line"

    def test_empty_dxf_raises(self, parser, tmp_path):
        dxf_file = tmp_path / "empty.dxf"
        dxf_file.write_text("", encoding="utf-8")

        with pytest.raises(parser.ParseError, match="empty or unreadable"):
            parser.parse_ascii_dxf(dxf_file, sample_limit=25, precision=6)

    def test_layers_collected_from_entities(self, parser, tmp_path):
        dxf_content = textwrap.dedent("""\
            0
            SECTION
            2
            ENTITIES
            0
            POINT
            5
            P1
            8
            GPS-CONTROL
            10
            50.0
            20
            60.0
            30
            0.0
            0
            ENDSEC
            0
            EOF
        """)
        dxf_file = tmp_path / "layers.dxf"
        dxf_file.write_text(dxf_content, encoding="utf-8")

        result = parser.parse_ascii_dxf(dxf_file, sample_limit=25, precision=6)
        assert "GPS-CONTROL" in result["summary"]["layers"]


# ===================================================================
# 5. Topology graph construction
# ===================================================================

class TestBuildTopology:
    def test_zero_length_edges_skipped(self, parser):
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
        topo = parser.build_topology(entities=entities, tolerance=1e-6, precision=6)
        assert topo["edge_count"] == 0

    def test_single_edge(self, parser):
        entities = [
            {
                "id": "line-1",
                "type": "LINE",
                "geometry": {
                    "kind": "line",
                    "start": [0.0, 0.0, 0.0],
                    "end": [10.0, 0.0, 0.0],
                },
            }
        ]
        topo = parser.build_topology(entities=entities, tolerance=1e-6, precision=6)
        assert topo["node_count"] == 2
        assert topo["edge_count"] == 1
        assert topo["connected_components"] == 1

    def test_two_connected_edges(self, parser):
        entities = [
            {
                "id": "line-1",
                "type": "LINE",
                "geometry": {"kind": "line", "start": [0, 0, 0], "end": [10, 0, 0]},
            },
            {
                "id": "line-2",
                "type": "LINE",
                "geometry": {"kind": "line", "start": [10, 0, 0], "end": [20, 0, 0]},
            },
        ]
        topo = parser.build_topology(entities=entities, tolerance=1e-6, precision=6)
        assert topo["node_count"] == 3
        assert topo["edge_count"] == 2
        assert topo["connected_components"] == 1

    def test_two_disconnected_components(self, parser):
        entities = [
            {
                "id": "line-1",
                "type": "LINE",
                "geometry": {"kind": "line", "start": [0, 0, 0], "end": [10, 0, 0]},
            },
            {
                "id": "line-2",
                "type": "LINE",
                "geometry": {"kind": "line", "start": [100, 100, 0], "end": [200, 100, 0]},
            },
        ]
        topo = parser.build_topology(entities=entities, tolerance=1e-6, precision=6)
        assert topo["connected_components"] == 2

    def test_loop_from_closed_polyline(self, parser):
        entities = [
            {
                "id": "poly-1",
                "type": "LWPOLYLINE",
                "geometry": {
                    "kind": "polyline",
                    "vertices": [[0, 0, 0], [10, 0, 0], [10, 10, 0], [0, 10, 0]],
                    "closed": True,
                },
            }
        ]
        topo = parser.build_topology(entities=entities, tolerance=1e-6, precision=6)
        assert topo["loop_count"] == 1
        assert topo["edge_count"] == 4

    def test_junction_node_detection(self, parser):
        entities = [
            {"id": "l1", "type": "LINE", "geometry": {"kind": "line", "start": [0, 0, 0], "end": [5, 5, 0]}},
            {"id": "l2", "type": "LINE", "geometry": {"kind": "line", "start": [5, 5, 0], "end": [10, 0, 0]}},
            {"id": "l3", "type": "LINE", "geometry": {"kind": "line", "start": [5, 5, 0], "end": [10, 10, 0]}},
        ]
        topo = parser.build_topology(entities=entities, tolerance=1e-6, precision=6)
        assert topo["junction_nodes"] >= 1

    def test_circle_loop(self, parser):
        entities = [
            {
                "id": "circle-1",
                "type": "CIRCLE",
                "geometry": {"kind": "circle", "center": [0, 0, 0], "radius": 10.0},
            }
        ]
        topo = parser.build_topology(entities=entities, tolerance=1e-6, precision=6)
        assert topo["loop_count"] == 1
        assert topo["edge_count"] == 0

    def test_arc_edge(self, parser):
        entities = [
            {
                "id": "arc-1",
                "type": "ARC",
                "geometry": {
                    "kind": "arc",
                    "center": [0, 0, 0],
                    "radius": 5.0,
                    "start_angle": 0.0,
                    "end_angle": 90.0,
                    "start": [5, 0, 0],
                    "end": [0, 5, 0],
                },
            }
        ]
        topo = parser.build_topology(entities=entities, tolerance=1e-6, precision=6)
        assert topo["edge_count"] == 1
        assert topo["edges"][0]["kind"] == "arc"

    def test_empty_entities(self, parser):
        topo = parser.build_topology(entities=[], tolerance=1e-6, precision=6)
        assert topo["node_count"] == 0
        assert topo["edge_count"] == 0
        assert topo["loop_count"] == 0
        assert topo["connected_components"] == 0

    def test_invalid_tolerance_raises(self, parser):
        with pytest.raises(parser.ParseError, match="tolerance must be > 0"):
            parser.build_topology(entities=[], tolerance=-1, precision=6)

    def test_adjacency_populated(self, parser):
        entities = [
            {"id": "l1", "type": "LINE", "geometry": {"kind": "line", "start": [0, 0, 0], "end": [10, 0, 0]}},
        ]
        topo = parser.build_topology(entities=entities, tolerance=1e-6, precision=6)
        assert len(topo["adjacency"]) > 0

    def test_tolerance_merges_close_points(self, parser):
        entities = [
            {"id": "l1", "type": "LINE", "geometry": {"kind": "line", "start": [0, 0, 0], "end": [10, 0, 0]}},
            {"id": "l2", "type": "LINE", "geometry": {"kind": "line", "start": [10.000001, 0, 0], "end": [20, 0, 0]}},
        ]
        topo = parser.build_topology(entities=entities, tolerance=0.001, precision=6)
        assert topo["node_count"] == 3
        assert topo["connected_components"] == 1


# ===================================================================
# 6. Civil survey summary and domain scoring
# ===================================================================

class TestBuildSurveyDomainCoverage:
    def _empty_inputs(self, parser):
        feature_counts = {
            "parcel_boundaries": 0, "centerlines": 0, "contours": 0,
            "spot_elevation_points": 0, "spot_elevation_labels": 0,
            "utility_entities": 0, "control_points": 0,
        }
        layer_groups: dict[str, list[str]] = {}
        summary: dict[str, Any] = {"entity_total": 0}
        topology: dict[str, Any] = {"edge_count": 0, "loop_count": 0, "connected_components": 0}
        return feature_counts, layer_groups, summary, topology

    def test_empty_input_all_none(self, parser):
        fc, lg, s, t = self._empty_inputs(parser)
        result = parser.build_survey_domain_coverage(fc, lg, s, t, set())
        for domain in result.values():
            assert domain["confidence"] == "none"
            assert domain["score"] == 0

    def test_parcels_boost_boundary_score(self, parser):
        fc, lg, s, t = self._empty_inputs(parser)
        fc["parcel_boundaries"] = 5
        lg["parcel_boundary"] = ["LOT-BOUNDARY"]
        t["loop_count"] = 5
        result = parser.build_survey_domain_coverage(fc, lg, s, t, set())
        boundary = result["boundary_retracement_surveys"]
        assert boundary["confidence"] in ("high", "medium")
        assert boundary["score"] >= 5

    def test_contours_boost_topo_score(self, parser):
        fc, lg, s, t = self._empty_inputs(parser)
        fc["contours"] = 10
        lg["contour"] = ["TOPO-CONTOUR"]
        fc["control_points"] = 2
        result = parser.build_survey_domain_coverage(fc, lg, s, t, set())
        topo_domain = result["site_topography_control_surveys"]
        assert topo_domain["confidence"] in ("high", "medium")

    def test_context_keywords_boost(self, parser):
        fc, lg, s, t = self._empty_inputs(parser)
        tokens = parser.collect_context_tokens(["boundary", "retracement", "alta"])
        result = parser.build_survey_domain_coverage(fc, lg, s, t, tokens)
        boundary = result["boundary_retracement_surveys"]
        assert boundary["score"] >= 1

    def test_construction_scoring(self, parser):
        fc, lg, s, t = self._empty_inputs(parser)
        fc["centerlines"] = 3
        fc["utility_entities"] = 5
        lg["construction"] = ["ASBUILT-ROAD"]
        t["edge_count"] = 10
        result = parser.build_survey_domain_coverage(fc, lg, s, t, set())
        const = result["construction_support_surveys"]
        assert const["confidence"] in ("high", "medium")

    def test_remote_sensing_large_dataset(self, parser):
        fc, lg, s, t = self._empty_inputs(parser)
        s["entity_total"] = 10000
        lg["remote_sensing"] = ["LIDAR-POINTS"]
        result = parser.build_survey_domain_coverage(fc, lg, s, t, set())
        remote = result["remote_specialized_surveying"]
        assert remote["score"] >= 2


class TestBuildCivilSurveySummary:
    def _build_entities(self) -> list[dict[str, Any]]:
        return [
            {
                "id": "parcel-1",
                "type": "LWPOLYLINE",
                "layer": "LOT-BOUNDARY",
                "geometry": {
                    "kind": "polyline",
                    "vertices": [
                        [0, 0, 0], [100, 0, 0], [100, 100, 0], [0, 100, 0],
                    ],
                    "closed": True,
                },
            },
            {
                "id": "cl-1",
                "type": "LINE",
                "layer": "ROAD-CENTERLINE",
                "geometry": {"kind": "line", "start": [0, 50, 0], "end": [100, 50, 0]},
            },
            {
                "id": "spot-1",
                "type": "POINT",
                "layer": "GPS-CONTROL",
                "geometry": {"kind": "point", "point": [50, 50, 100.5]},
            },
            {
                "id": "contour-1",
                "type": "LWPOLYLINE",
                "layer": "TOPO-CONTOUR",
                "geometry": {
                    "kind": "polyline",
                    "vertices": [[0, 0, 100], [100, 0, 100]],
                    "closed": False,
                },
            },
            {
                "id": "util-1",
                "type": "LINE",
                "layer": "WATER-MAIN",
                "geometry": {"kind": "line", "start": [10, 10, 0], "end": [90, 10, 0]},
            },
            {
                "id": "elev-text-1",
                "type": "TEXT",
                "layer": "SPOT-ELEV",
                "geometry": {"kind": "text", "point": [20, 20, 0], "text": "EL=105.3"},
            },
        ]

    def test_feature_counts_populated(self, parser):
        entities = self._build_entities()
        topology = parser.build_topology(entities, tolerance=1e-6, precision=6)
        summary: dict[str, Any] = {
            "entity_total": len(entities),
            "layers": ["LOT-BOUNDARY", "ROAD-CENTERLINE", "GPS-CONTROL", "TOPO-CONTOUR", "WATER-MAIN", "SPOT-ELEV"],
            "insunits": 6,
        }
        civil = parser.build_civil_survey_summary(
            entities=entities,
            topology=topology,
            summary=summary,
            precision=6,
            sample_limit=25,
            input_path=Path("/fake/survey.dxf"),
            project_tags=["boundary"],
        )

        assert civil["feature_counts"]["parcel_boundaries"] >= 1
        assert civil["feature_counts"]["centerlines"] >= 1
        assert civil["feature_counts"]["contours"] >= 1
        assert civil["feature_counts"]["control_points"] >= 1
        assert civil["feature_counts"]["utility_entities"] >= 1

    def test_parcels_section(self, parser):
        entities = self._build_entities()
        topology = parser.build_topology(entities, tolerance=1e-6, precision=6)
        summary: dict[str, Any] = {
            "entity_total": len(entities),
            "layers": ["LOT-BOUNDARY"],
            "insunits": 6,
        }
        civil = parser.build_civil_survey_summary(
            entities=entities, topology=topology, summary=summary,
            precision=6, sample_limit=25, input_path=Path("/fake/test.dxf"),
            project_tags=[],
        )
        assert civil["parcels"]["count"] >= 1
        assert civil["parcels"]["total_area_sq"] > 0

    def test_terrain_section(self, parser):
        entities = self._build_entities()
        topology = parser.build_topology(entities, tolerance=1e-6, precision=6)
        summary: dict[str, Any] = {
            "entity_total": len(entities),
            "layers": ["TOPO-CONTOUR"],
            "insunits": None,
        }
        civil = parser.build_civil_survey_summary(
            entities=entities, topology=topology, summary=summary,
            precision=6, sample_limit=25, input_path=Path("/fake/topo.dxf"),
            project_tags=[],
        )
        assert civil["terrain"]["contour_count"] >= 1

    def test_qa_flags_on_missing_units(self, parser):
        entities = self._build_entities()
        topology = parser.build_topology(entities, tolerance=1e-6, precision=6)
        summary: dict[str, Any] = {
            "entity_total": len(entities),
            "layers": [],
            "insunits": None,
        }
        civil = parser.build_civil_survey_summary(
            entities=entities, topology=topology, summary=summary,
            precision=6, sample_limit=25, input_path=Path("/fake/test.dxf"),
            project_tags=[],
        )
        assert any("INSUNITS" in flag for flag in civil["qa_flags"])

    def test_survey_domains_present(self, parser):
        entities = self._build_entities()
        topology = parser.build_topology(entities, tolerance=1e-6, precision=6)
        summary: dict[str, Any] = {
            "entity_total": len(entities),
            "layers": ["LOT-BOUNDARY", "TOPO-CONTOUR"],
            "insunits": 6,
        }
        civil = parser.build_civil_survey_summary(
            entities=entities, topology=topology, summary=summary,
            precision=6, sample_limit=25, input_path=Path("/fake/survey.dxf"),
            project_tags=[],
        )
        domains = civil["survey_domains"]
        assert "boundary_retracement_surveys" in domains
        assert "site_topography_control_surveys" in domains
        assert "construction_support_surveys" in domains

    def test_spot_elevation_labels_detected(self, parser):
        entities = self._build_entities()
        topology = parser.build_topology(entities, tolerance=1e-6, precision=6)
        summary: dict[str, Any] = {
            "entity_total": len(entities),
            "layers": ["SPOT-ELEV"],
            "insunits": 6,
        }
        civil = parser.build_civil_survey_summary(
            entities=entities, topology=topology, summary=summary,
            precision=6, sample_limit=25, input_path=Path("/fake/test.dxf"),
            project_tags=[],
        )
        assert civil["feature_counts"]["spot_elevation_labels"] >= 1
        assert civil["spot_elevations"]["label_count"] >= 1


# ===================================================================
# 7. Converter command building and input validation
# ===================================================================

class TestBuildConverterCommand:
    def test_basic_template(self, parser):
        parts, text = parser.build_converter_command(
            "convert {input} {output}",
            Path("/in/file.dwg"),
            Path("/out/file.dxf"),
        )
        assert parts[0] == "convert"
        assert "/in/file.dwg" in text
        assert "/out/file.dxf" in text

    def test_output_dir_placeholder(self, parser):
        parts, text = parser.build_converter_command(
            "conv {input} --outdir {output_dir} --name {output_stem} {output}",
            Path("/in/test.dwg"),
            Path("/out/dir/result.dxf"),
        )
        assert "/out/dir" in text
        assert "result" in text

    def test_missing_input_placeholder_raises(self, parser):
        with pytest.raises(parser.ParseError, match="must include"):
            parser.build_converter_command(
                "convert {output}",
                Path("/in/file.dwg"),
                Path("/out/file.dxf"),
            )

    def test_missing_output_placeholder_raises(self, parser):
        with pytest.raises(parser.ParseError, match="must include"):
            parser.build_converter_command(
                "convert {input}",
                Path("/in/file.dwg"),
                Path("/out/file.dxf"),
            )

    def test_unknown_placeholder_raises(self, parser):
        with pytest.raises(parser.ParseError, match="Unknown converter placeholder"):
            parser.build_converter_command(
                "convert {input} {output} {bogus}",
                Path("/in/file.dwg"),
                Path("/out/file.dxf"),
            )


class TestParseInput:
    def test_unsupported_extension_raises(self, parser, tmp_path):
        bad_file = tmp_path / "file.txt"
        bad_file.write_text("hello")
        with pytest.raises(parser.ParseError, match="Unsupported input type"):
            parser.parse_input(bad_file, converter_template=None, sample_limit=25, precision=6)

    def test_dwg_without_converter_raises(self, parser, tmp_path):
        dwg_file = tmp_path / "file.dwg"
        dwg_file.write_bytes(b"\x00" * 100)
        with pytest.raises(parser.ParseError, match="converter command"):
            parser.parse_input(dwg_file, converter_template=None, sample_limit=25, precision=6)

    def test_nonexistent_file_raises(self, parser, tmp_path):
        fake = tmp_path / "nonexistent.dxf"
        with pytest.raises(parser.ParseError, match="not a file"):
            parser.parse_input(fake, converter_template=None, sample_limit=25, precision=6)

    def test_empty_file_raises(self, parser, tmp_path):
        empty = tmp_path / "empty.dxf"
        empty.write_bytes(b"")
        with pytest.raises(parser.ParseError, match="empty"):
            parser.parse_input(empty, converter_template=None, sample_limit=25, precision=6)

    def test_binary_dxf_raises(self, parser, tmp_path):
        binary_file = tmp_path / "binary.dxf"
        binary_file.write_bytes(b"\x00\x01\x02" * 100)
        with pytest.raises(parser.ParseError, match="binary"):
            parser.parse_input(binary_file, converter_template=None, sample_limit=25, precision=6)

    def test_dxf_uses_fallback_parser(self, parser, tmp_path):
        dxf_content = textwrap.dedent("""\
            0
            SECTION
            2
            ENTITIES
            0
            LINE
            5
            L1
            8
            TEST
            10
            0.0
            20
            0.0
            30
            0.0
            11
            10.0
            21
            10.0
            31
            0.0
            0
            ENDSEC
            0
            EOF
        """)
        dxf_file = tmp_path / "test.dxf"
        dxf_file.write_text(dxf_content, encoding="utf-8")

        result, conversion_info, temp_dir = parser.parse_input(
            dxf_file, converter_template=None, sample_limit=25, precision=6,
        )
        assert conversion_info["used"] is False
        assert len(result["entities"]) == 1
        assert temp_dir is None


class TestWriteReport:
    def test_writes_json_file(self, parser, tmp_path):
        report = {"test": "value", "count": 42}
        output_path = tmp_path / "report.json"
        parser.write_report(report, output_path)

        assert output_path.exists()
        loaded = json.loads(output_path.read_text())
        assert loaded["test"] == "value"
        assert loaded["count"] == 42

    def test_creates_parent_dirs(self, parser, tmp_path):
        output_path = tmp_path / "nested" / "dir" / "report.json"
        parser.write_report({"a": 1}, output_path)
        assert output_path.exists()

    def test_stdout_when_no_path(self, parser, capsys):
        parser.write_report({"hello": "world"}, None)
        captured = capsys.readouterr()
        assert "hello" in captured.out
        assert "world" in captured.out


# ===================================================================
# 8. End-to-end integration: full parse pipeline on synthetic DXF
# ===================================================================

class TestEndToEnd:
    def _make_survey_dxf(self, tmp_path: Path) -> Path:
        dxf_content = textwrap.dedent("""\
            0
            SECTION
            2
            HEADER
            9
            $ACADVER
            1
            AC1032
            9
            $INSUNITS
            70
            6
            0
            ENDSEC
            0
            SECTION
            2
            TABLES
            0
            TABLE
            2
            LAYER
            0
            LAYER
            2
            LOT-BOUNDARY
            0
            LAYER
            2
            ROAD-CENTERLINE
            0
            LAYER
            2
            TOPO-CONTOUR
            0
            LAYER
            2
            GPS-CONTROL
            0
            LAYER
            2
            WATER-MAIN
            0
            ENDTAB
            0
            ENDSEC
            0
            SECTION
            2
            ENTITIES
            0
            LWPOLYLINE
            5
            PL1
            8
            LOT-BOUNDARY
            70
            1
            10
            0.0
            20
            0.0
            10
            100.0
            20
            0.0
            10
            100.0
            20
            100.0
            10
            0.0
            20
            100.0
            0
            LINE
            5
            CL1
            8
            ROAD-CENTERLINE
            10
            0.0
            20
            50.0
            30
            0.0
            11
            100.0
            21
            50.0
            31
            0.0
            0
            LINE
            5
            CTR1
            8
            TOPO-CONTOUR
            10
            0.0
            20
            0.0
            30
            100.0
            11
            100.0
            21
            0.0
            31
            100.0
            0
            LINE
            5
            CTR2
            8
            TOPO-CONTOUR
            10
            0.0
            20
            10.0
            30
            101.0
            11
            100.0
            21
            10.0
            31
            101.0
            0
            POINT
            5
            CP1
            8
            GPS-CONTROL
            10
            50.0
            20
            50.0
            30
            100.5
            0
            LINE
            5
            UT1
            8
            WATER-MAIN
            10
            10.0
            20
            10.0
            30
            0.0
            11
            90.0
            21
            10.0
            31
            0.0
            0
            ENDSEC
            0
            EOF
        """)
        dxf_file = tmp_path / "survey_test.dxf"
        dxf_file.write_text(dxf_content, encoding="utf-8")
        return dxf_file

    def test_full_pipeline(self, parser, tmp_path):
        dxf_file = self._make_survey_dxf(tmp_path)
        output_file = tmp_path / "output.json"

        result, conversion_info, temp_dir = parser.parse_input(
            dxf_file, converter_template=None, sample_limit=25, precision=6,
        )

        all_entities = result["entities"]
        topology = parser.build_topology(
            entities=all_entities, tolerance=1e-6, precision=6,
        )

        summary = dict(result["summary"])
        summary["geometry_entities_total"] = sum(
            1 for e in all_entities if isinstance(e.get("geometry"), dict)
        )

        civil = parser.build_civil_survey_summary(
            entities=all_entities,
            topology=topology,
            summary=summary,
            precision=6,
            sample_limit=25,
            input_path=dxf_file,
            project_tags=["boundary", "topo"],
        )

        report = {
            "input_file": str(dxf_file),
            "summary": summary,
            "entities_total": len(all_entities),
            "topology": topology,
            "civil_survey": civil,
        }

        parser.write_report(report, output_file)
        assert output_file.exists()

        loaded = json.loads(output_file.read_text())

        assert loaded["entities_total"] >= 6
        assert loaded["summary"]["dxf_version"] == "AC1032"
        assert loaded["summary"]["insunits"] == 6

        assert loaded["topology"]["node_count"] > 0
        assert loaded["topology"]["edge_count"] > 0

        fc = loaded["civil_survey"]["feature_counts"]
        assert fc["parcel_boundaries"] >= 1
        assert fc["centerlines"] >= 1
        assert fc["contours"] >= 1
        assert fc["control_points"] >= 1
        assert fc["utility_entities"] >= 1

        assert loaded["civil_survey"]["parcels"]["count"] >= 1
        assert loaded["civil_survey"]["terrain"]["contour_count"] >= 1

        domains = loaded["civil_survey"]["survey_domains"]
        assert "boundary_retracement_surveys" in domains
        assert "site_topography_control_surveys" in domains
        assert domains["boundary_retracement_surveys"]["score"] >= 1
        assert domains["site_topography_control_surveys"]["score"] >= 1

    def test_layer_table_parsed(self, parser, tmp_path):
        dxf_file = self._make_survey_dxf(tmp_path)
        result, _, _ = parser.parse_input(
            dxf_file, converter_template=None, sample_limit=25, precision=6,
        )
        layers = result["summary"]["layers"]
        assert "LOT-BOUNDARY" in layers
        assert "ROAD-CENTERLINE" in layers
        assert "GPS-CONTROL" in layers
