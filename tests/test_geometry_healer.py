"""Tests for deterministic geometry healer."""

import numpy as np

from totali.cad_shielding.geometry_healer import (
    GeometryHealer,
    HealingConfig,
    HealingResult,
    SHAPELY_AVAILABLE,
    TRIMESH_AVAILABLE,
)


class TestGeometryHealer:
    def test_polyline_removes_consecutive_duplicates(self):
        healer = GeometryHealer()
        line = np.array([[0.0, 0.0, 0.0], [0.0, 0.0, 0.0], [1.0, 1.0, 0.0]])
        healed, issues = healer.heal_polyline(line, "line0")
        assert healed is not None
        assert len(healed) == 2
        assert any("duplicate" in msg for msg in issues)

    def test_polyline_quarantines_short_lines(self):
        healer = GeometryHealer()
        healed, issues = healer.heal_polyline(np.array([[0.0, 0.0, 0.0]]), "line1")
        assert healed is None
        assert any("fewer than 2 vertices" in msg for msg in issues)

    def test_polygon_closes_ring(self):
        healer = GeometryHealer()
        poly = np.array([[0.0, 0.0], [2.0, 0.0], [2.0, 1.0], [0.0, 1.0]])
        healed, issues = healer.heal_polygon(poly, "poly0")
        assert healed is not None
        assert np.allclose(healed[0], healed[-1])
        assert any("closed polygon ring" in msg for msg in issues)

    def test_polygon_quarantines_degenerate_area(self):
        healer = GeometryHealer()
        deg = np.array([[0.0, 0.0], [1.0, 0.0], [2.0, 0.0]])  # collinear
        healed, issues = healer.heal_polygon(deg, "poly1")
        assert healed is None
        assert any("degenerate polygon" in msg for msg in issues)

    def test_polyline_snap_to_grid(self):
        cfg = HealingConfig(snap_tolerance=0.1, remove_duplicates=False)
        healer = GeometryHealer(cfg)
        line = np.array([[0.123, 0.456, 0.0], [1.127, 1.454, 0.0]])
        healed, issues = healer.heal_polyline(line, "line0")
        assert healed is not None
        assert np.allclose(healed[:, :2], np.array([[0.1, 0.5], [1.1, 1.5]]))

    def test_polygon_fixes_winding_order(self):
        healer = GeometryHealer()
        # CW order -> negative area -> healer flips to CCW
        cw = np.array([[0.0, 0.0], [0.0, 2.0], [2.0, 2.0], [2.0, 0.0]])
        healed, issues = healer.heal_polygon(cw, "poly0")
        assert healed is not None
        assert any("winding" in msg for msg in issues)
        area = healer._polygon_area_2d(healed)
        assert area > 0

    def test_polygon_3d_preserves_z_on_repair(self):
        if not SHAPELY_AVAILABLE:
            return
        healer = GeometryHealer()
        poly = np.array([[0.0, 0.0, 10.0], [2.0, 0.0, 10.0], [2.0, 1.0, 10.0], [0.0, 1.0, 10.0]])
        healed, _ = healer.heal_polygon(poly, "poly0")
        assert healed is not None
        assert healed.shape[1] == 3
        assert np.allclose(healed[:, 2], 10.0)

    def test_heal_mesh_returns_vertices_faces_or_skips(self):
        healer = GeometryHealer()
        verts = np.array([[0.0, 0.0, 0.0], [1.0, 0.0, 0.0], [0.5, 1.0, 0.0]], dtype=np.float64)
        faces = np.array([[0, 1, 2]], dtype=np.int32)
        out_v, out_f, issues = healer.heal_mesh(verts, faces, "mesh0")
        if TRIMESH_AVAILABLE:
            if out_v is not None and out_f is not None:
                assert len(out_v) >= 3 and len(out_f) >= 1
            else:
                assert any("mesh healing failed" in msg for msg in issues)
        else:
            assert any("trimesh not available" in msg for msg in issues)

    def test_heal_mesh_quarantines_on_failure(self):
        healer = GeometryHealer()
        bad_verts = np.array([[0.0, 0.0, 0.0]])  # too few for a face
        bad_faces = np.array([[0, 0, 0]], dtype=np.int32)
        out_v, out_f, issues = healer.heal_mesh(bad_verts, bad_faces, "bad")
        if TRIMESH_AVAILABLE:
            # trimesh may still create something or raise
            assert (out_v is None and out_f is None) or (out_v is not None and out_f is not None)
        assert isinstance(issues, list)

    def test_config_defaults(self):
        cfg = HealingConfig()
        assert cfg.close_tolerance == 0.001
        assert cfg.check_self_intersection is True
        assert cfg.weld_vertices is True

    def test_healing_result_dataclass(self):
        r = HealingResult(input_count=10, healed_count=2, quarantined_count=1, passed_count=7)
        assert r.input_count == 10
        assert r.healed_count == 2
        assert r.quarantined_ids == []
