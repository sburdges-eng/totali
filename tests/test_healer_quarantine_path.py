"""C-2: geometry healer quarantines unhealable input rather than writing it.

Invariant: geometry that cannot be healed never flows downstream to CAD. It
goes to the quarantine side-file with a recorded entity_id + reason.
"""

import numpy as np

from totali.cad_shielding.geometry_healer import GeometryHealer, HealingConfig


class TestDegeneratePolylines:
    def test_single_vertex_rejected(self):
        healer = GeometryHealer(HealingConfig())
        result, issues = healer.heal_polyline(
            np.array([[0.0, 0.0, 0.0]]), entity_id="bl-single"
        )
        assert result is None
        assert any("bl-single" in i for i in issues)

    def test_zero_vertices_rejected(self):
        healer = GeometryHealer(HealingConfig())
        result, issues = healer.heal_polyline(
            np.zeros((0, 3)), entity_id="bl-empty"
        )
        assert result is None
        assert issues

    def test_all_duplicate_vertices_rejected(self):
        healer = GeometryHealer(HealingConfig(remove_duplicates=True))
        same = np.tile([[1.0, 2.0, 3.0]], (5, 1))
        result, issues = healer.heal_polyline(same, entity_id="bl-dup")
        assert result is None
        assert any("bl-dup" in i for i in issues)


class TestPolylineHealsAndReportsIssues:
    def test_healthy_polyline_passes(self):
        healer = GeometryHealer(HealingConfig())
        verts = np.array([[0.0, 0.0, 0.0], [1.0, 0.0, 0.0], [2.0, 1.0, 0.0]])
        result, issues = healer.heal_polyline(verts, entity_id="bl-ok")
        assert result is not None
        assert result.shape[1] == 3

    def test_duplicate_vertices_removed(self):
        healer = GeometryHealer(HealingConfig(remove_duplicates=True))
        verts = np.array(
            [[0.0, 0.0, 0.0], [0.0, 0.0, 0.0], [1.0, 0.0, 0.0]]
        )
        result, issues = healer.heal_polyline(verts, entity_id="bl-dup-once")
        assert result is not None
        assert len(result) == 2
        assert any("duplicate" in i for i in issues)


class TestPolygonDegenerates:
    def test_too_few_vertices(self):
        healer = GeometryHealer(HealingConfig())
        result, issues = healer.heal_polygon(
            np.array([[0.0, 0.0, 0.0], [1.0, 1.0, 0.0]]), entity_id="pg-thin"
        )
        assert result is None
        assert any("pg-thin" in i for i in issues)

    def test_zero_vertices(self):
        healer = GeometryHealer(HealingConfig())
        result, issues = healer.heal_polygon(
            np.zeros((0, 3)), entity_id="pg-empty"
        )
        assert result is None
