"""C-7: CAD Shielding — DXF entity-ordering determinism.

Invariant: given the same ExtractionResult input, two independent calls to
CADShield._write_dxf_manual produce semantically identical DXF output —
same entity count, same layer distribution, same geometric coordinate
sequence in the same order.

NOTE ON ENTITY ID NON-DETERMINISM
----------------------------------
CADShield.__init__ assigns::

    self._id_prefix = uuid.uuid4().hex[:6]
    self._id_counter = itertools.count()

Each CADShield instance therefore produces different entity IDs (e.g.
"a3f201000000" vs "7c91d0000000") even for identical input. This is
intentional — IDs are chain-of-custody tokens, not content fingerprints.
These tests deliberately do NOT assert ID equality. The content-
deterministic fields we DO assert: entity_count, entity type sequence,
layer sequence, source_hash sequence, and the geometric coordinate list
written to the DXF file.
"""

from pathlib import Path

import numpy as np
import pytest

from totali.cad_shielding.shield import CADShield
from totali.pipeline.models import ExtractionResult


def _make_shield(sample_config, audit_logger) -> CADShield:
    return CADShield(sample_config["cad_shielding"], audit_logger)


def _fixed_extraction() -> ExtractionResult:
    """Three breaklines of varying length → 2 + 3 + 1 = 6 LINE entities total.
    All literal floats — no RNG dependency."""
    return ExtractionResult(
        breaklines=[
            np.array([[1.0, 2.0, 3.0], [4.0, 5.0, 6.0], [7.0, 8.0, 9.0]]),
            np.array([
                [10.0, 20.0, 30.0],
                [11.0, 21.0, 31.0],
                [12.0, 22.0, 32.0],
                [13.0, 23.0, 33.0],
            ]),
            np.array([[100.0, 200.0, 300.0], [101.0, 201.0, 301.0]]),
        ],
        contours_minor=[],
        contours_index=[],
        building_footprints=[],
        curb_lines=[],
        wire_lines=[],
        hardscape_polygons=[],
    )


_EXPECTED_ENTITY_COUNT = 6


def _parse_dxf_lines(dxf_path: Path) -> list[tuple]:
    """Parse LINE entities from a manual-writer DXF file.

    Returns (x0, y0, z0, x1, y1, z1) tuples in document order. Plain text
    parsing of the group-code format produced by CADShield._write_dxf_manual;
    no ezdxf dependency.
    """
    raw = dxf_path.read_text().splitlines()
    pairs = list(zip(raw[0::2], raw[1::2]))

    coords: list[tuple] = []
    inside_line = False
    buf: dict[str, float] = {}

    for code, value in pairs:
        code = code.strip()
        value = value.strip()

        if code == "0":
            if inside_line:
                coords.append((
                    buf["10"], buf["20"], buf["30"],
                    buf["11"], buf["21"], buf["31"],
                ))
                buf = {}
            inside_line = value == "LINE"
        elif inside_line and code in ("10", "20", "30", "11", "21", "31"):
            buf[code] = float(value)

    if inside_line and len(buf) == 6:
        coords.append((
            buf["10"], buf["20"], buf["30"],
            buf["11"], buf["21"], buf["31"],
        ))

    return coords


@pytest.fixture
def extraction() -> ExtractionResult:
    return _fixed_extraction()


@pytest.fixture
def shield_a(sample_config, audit_logger) -> CADShield:
    return _make_shield(sample_config, audit_logger)


@pytest.fixture
def shield_b(sample_config, audit_logger) -> CADShield:
    return _make_shield(sample_config, audit_logger)


class TestEntityOrderingDeterministic:
    """C-7: manifest-level fields are content-deterministic across instances."""

    def test_entity_count_is_identical(self, shield_a, shield_b, extraction, tmp_path):
        m1 = shield_a._write_dxf_manual(extraction, tmp_path / "a.dxf")
        m2 = shield_b._write_dxf_manual(extraction, tmp_path / "b.dxf")
        assert m1["entity_count"] == m2["entity_count"] == _EXPECTED_ENTITY_COUNT

    def test_entity_type_sequence_is_identical(self, shield_a, shield_b, extraction, tmp_path):
        m1 = shield_a._write_dxf_manual(extraction, tmp_path / "a.dxf")
        m2 = shield_b._write_dxf_manual(extraction, tmp_path / "b.dxf")
        types_1 = [e["type"] for e in m1["entities"]]
        types_2 = [e["type"] for e in m2["entities"]]
        assert types_1 == types_2
        assert all(t == "LINE" for t in types_1)

    def test_layer_sequence_is_identical(self, shield_a, shield_b, extraction, tmp_path):
        m1 = shield_a._write_dxf_manual(extraction, tmp_path / "a.dxf")
        m2 = shield_b._write_dxf_manual(extraction, tmp_path / "b.dxf")
        layers_1 = [e["layer"] for e in m1["entities"]]
        layers_2 = [e["layer"] for e in m2["entities"]]
        assert layers_1 == layers_2
        assert all(lyr == "TOTaLi-SURV-BRKLN-DRAFT" for lyr in layers_1)

    def test_source_hash_sequence_is_identical(self, shield_a, shield_b, extraction, tmp_path):
        m1 = shield_a._write_dxf_manual(extraction, tmp_path / "a.dxf")
        m2 = shield_b._write_dxf_manual(extraction, tmp_path / "b.dxf")
        hashes_1 = [e["source_hash"] for e in m1["entities"]]
        hashes_2 = [e["source_hash"] for e in m2["entities"]]
        assert hashes_1 == hashes_2

    def test_entity_ids_differ_across_instances(self, shield_a, shield_b, extraction, tmp_path):
        """Pins the known non-determinism. If IDs become identical across
        instances the uuid4 prefix has been accidentally made deterministic —
        a chain-of-custody regression worth investigating."""
        m1 = shield_a._write_dxf_manual(extraction, tmp_path / "a.dxf")
        m2 = shield_b._write_dxf_manual(extraction, tmp_path / "b.dxf")
        ids_1 = [e["id"] for e in m1["entities"]]
        ids_2 = [e["id"] for e in m2["entities"]]
        assert len(set(ids_1)) == len(ids_1)
        assert len(set(ids_2)) == len(ids_2)
        assert ids_1 != ids_2


class TestCoordinateSequenceDeterministic:
    """C-7: DXF file content — geometric coordinates written in input order."""

    def test_dxf_coordinate_sequence_matches_across_instances(
        self, shield_a, shield_b, extraction, tmp_path
    ):
        path_a = tmp_path / "a.dxf"
        path_b = tmp_path / "b.dxf"
        shield_a._write_dxf_manual(extraction, path_a)
        shield_b._write_dxf_manual(extraction, path_b)

        coords_a = _parse_dxf_lines(path_a)
        coords_b = _parse_dxf_lines(path_b)

        assert len(coords_a) == len(coords_b) == _EXPECTED_ENTITY_COUNT
        for i, (ca, cb) in enumerate(zip(coords_a, coords_b)):
            assert ca == pytest.approx(cb), (
                f"Coordinate mismatch at LINE entity {i}: {ca} != {cb}"
            )

    def test_dxf_coordinate_sequence_matches_input_order(
        self, shield_a, extraction, tmp_path
    ):
        path = tmp_path / "ordered.dxf"
        shield_a._write_dxf_manual(extraction, path)
        coords = _parse_dxf_lines(path)

        expected: list[tuple] = []
        for brk in extraction.breaklines:
            for i in range(len(brk) - 1):
                p0, p1 = brk[i], brk[i + 1]
                expected.append((
                    float(p0[0]), float(p0[1]), float(p0[2]),
                    float(p1[0]), float(p1[1]), float(p1[2]),
                ))

        assert len(coords) == len(expected)
        for i, (actual, exp) in enumerate(zip(coords, expected)):
            assert actual == pytest.approx(exp), (
                f"Segment {i}: expected {exp}, got {actual}"
            )
