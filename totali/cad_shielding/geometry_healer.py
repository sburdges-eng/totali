"""Deterministic geometry healing helpers (C-2).

Ported from `main` during the branch/main reconciliation. The branch previously
folded a *count-only* diagnostic into `shield._heal_geometry`, which never
repaired geometry and let degenerate/unhealable entities flow to the DXF. This
module restores real healing — duplicate removal, ring closing, winding-order
fix, degenerate rejection, and (when shapely/trimesh are present) self-
intersection repair and mesh weld/hole-fill — returning ``None`` for geometry
that cannot be healed so the caller can EXCLUDE it from the deliverable.

Heavy deps are optional: `shapely` (polyline/polygon validity repair) and
`trimesh` (mesh healing) are imported behind availability flags so the fully
mocked test suite / CI (which install neither) degrade gracefully — numpy-only
healing and quarantine still work; the advanced repairs simply no-op.

Note vs main: ``snap_tolerance`` defaults to ``0.0`` (off) here. Grid-snapping
mutates *every* coordinate (not just bad geometry); it stays opt-in so healing
doesn't silently perturb measured survey coordinates.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import numpy as np

try:
    from shapely.geometry import LineString, Polygon
    from shapely.validation import make_valid

    SHAPELY_AVAILABLE = True
except ImportError:
    SHAPELY_AVAILABLE = False

try:
    import trimesh
    from trimesh.repair import fill_holes, fix_normals

    TRIMESH_AVAILABLE = True
except ImportError:
    TRIMESH_AVAILABLE = False


@dataclass
class HealingConfig:
    close_tolerance: float = 0.001
    degenerate_threshold: float = 0.0001
    snap_tolerance: float = 0.0  # off by default — snapping mutates all coords
    check_self_intersection: bool = True
    repair_self_intersection: bool = True
    weld_vertices: bool = True
    remove_duplicates: bool = True
    close_polygons: bool = True


@dataclass
class HealingResult:
    input_count: int = 0
    healed_count: int = 0
    quarantined_count: int = 0
    passed_count: int = 0
    issues: list[str] = field(default_factory=list)
    quarantined_ids: list[str] = field(default_factory=list)


class GeometryHealer:
    def __init__(self, config: HealingConfig | None = None):
        self.cfg = config or HealingConfig()

    def heal_polyline(
        self, vertices: np.ndarray, entity_id: str
    ) -> tuple[np.ndarray | None, list[str]]:
        """Heal one polyline. Returns (healed_vertices, issues) or (None, issues)
        when the polyline is unhealable (must be excluded from the deliverable)."""
        issues: list[str] = []
        if len(vertices) < 2:
            return None, [f"{entity_id}: fewer than 2 vertices"]

        if self.cfg.remove_duplicates:
            vertices, removed = self._remove_consecutive_dupes(vertices)
            if removed > 0:
                issues.append(f"{entity_id}: removed {removed} duplicate vertices")
        if len(vertices) < 2:
            return None, [f"{entity_id}: degenerate after duplicate removal"]

        if self.cfg.snap_tolerance > 0:
            vertices = self._snap_to_grid(vertices, self.cfg.snap_tolerance)

        if self.cfg.check_self_intersection and SHAPELY_AVAILABLE:
            line = LineString(vertices[:, :2])
            if not line.is_simple:
                issues.append(f"{entity_id}: self-intersecting polyline")
                if self.cfg.repair_self_intersection:
                    repaired = make_valid(line)
                    if repaired.geom_type == "LineString":
                        repaired_xy = np.array(repaired.coords)
                        if repaired_xy.size > 0:
                            vertices = self._restore_z(vertices, repaired_xy)
                            issues.append(f"{entity_id}: repaired self-intersection")

        return vertices, issues

    def heal_polygon(
        self, vertices: np.ndarray, entity_id: str
    ) -> tuple[np.ndarray | None, list[str]]:
        """Heal one polygon. Returns (healed_vertices, issues) or (None, issues)
        when the polygon is degenerate/unhealable."""
        issues: list[str] = []
        if len(vertices) < 3:
            return None, [f"{entity_id}: fewer than 3 vertices"]

        if self.cfg.remove_duplicates:
            vertices, removed = self._remove_consecutive_dupes(vertices)
            if removed > 0:
                issues.append(f"{entity_id}: removed {removed} duplicate vertices")
        if len(vertices) < 3:
            return None, [f"{entity_id}: degenerate after duplicate removal"]

        if self.cfg.close_polygons and (
            np.linalg.norm(vertices[0] - vertices[-1]) > self.cfg.close_tolerance
        ):
            vertices = np.vstack([vertices, vertices[0]])
            issues.append(f"{entity_id}: closed polygon ring")

        area = self._polygon_area_2d(vertices)
        if abs(area) < self.cfg.degenerate_threshold:
            return None, [f"{entity_id}: degenerate polygon (area={area:.8f})"]

        if area < 0:
            vertices = vertices[::-1]
            issues.append(f"{entity_id}: fixed winding order")

        if SHAPELY_AVAILABLE and self.cfg.check_self_intersection:
            poly = Polygon(vertices[:, :2])
            if not poly.is_valid:
                issues.append(f"{entity_id}: invalid polygon geometry")
                if self.cfg.repair_self_intersection:
                    repaired = make_valid(poly)
                    if repaired.geom_type == "Polygon" and not repaired.is_empty:
                        repaired_xy = np.array(repaired.exterior.coords)
                        vertices = self._restore_z(vertices, repaired_xy)
                        issues.append(f"{entity_id}: repaired via make_valid")

        return vertices, issues

    def heal_mesh(
        self, vertices: np.ndarray, faces: np.ndarray, entity_id: str
    ) -> tuple[np.ndarray | None, np.ndarray | None, list[str]]:
        """Heal a triangle mesh (weld, normals, hole-fill). Without trimesh,
        returns the input unchanged with a note (degenerate-face filtering is
        still applied by the caller)."""
        issues: list[str] = []
        if not TRIMESH_AVAILABLE:
            return vertices, faces, [f"{entity_id}: trimesh not available, skipping mesh healing"]
        try:
            mesh = trimesh.Trimesh(vertices=vertices, faces=faces, process=False)
            initial_faces = len(mesh.faces)
            mesh.remove_degenerate_faces()
            if len(mesh.faces) < initial_faces:
                issues.append(f"{entity_id}: removed {initial_faces - len(mesh.faces)} degenerate faces")
            if self.cfg.weld_vertices:
                mesh.merge_vertices()
            fix_normals(mesh)
            if not mesh.is_watertight:
                issues.append(f"{entity_id}: mesh not watertight")
                fill_holes(mesh)
                if mesh.is_watertight:
                    issues.append(f"{entity_id}: filled holes, now watertight")
            return np.asarray(mesh.vertices), np.asarray(mesh.faces), issues
        except Exception as exc:  # pragma: no cover - defensive path
            return None, None, [f"{entity_id}: mesh healing failed: {exc}"]

    @staticmethod
    def _restore_z(original: np.ndarray, repaired_xy: np.ndarray) -> np.ndarray:
        """Reattach a constant Z (mean of the original) to a 2D-repaired ring."""
        if original.shape[1] >= 3:
            z_val = float(np.mean(original[:, 2]))
            return np.column_stack([repaired_xy, np.full(len(repaired_xy), z_val)])
        return repaired_xy

    def _remove_consecutive_dupes(self, vertices: np.ndarray) -> tuple[np.ndarray, int]:
        if len(vertices) < 2:
            return vertices, 0
        diffs = np.linalg.norm(np.diff(vertices[:, :2], axis=0), axis=1)
        keep = np.concatenate([[True], diffs >= self.cfg.close_tolerance])
        return vertices[keep], int(np.sum(~keep))

    def _snap_to_grid(self, vertices: np.ndarray, tolerance: float) -> np.ndarray:
        return np.round(vertices / tolerance) * tolerance

    def _polygon_area_2d(self, vertices: np.ndarray) -> float:
        x = vertices[:, 0]
        y = vertices[:, 1]
        return float(0.5 * np.sum(x[:-1] * y[1:] - x[1:] * y[:-1]))
