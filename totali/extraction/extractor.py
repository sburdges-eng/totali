"""
Phase 3: Deterministic Feature Extraction
========================================
Performs geometry extraction from classified point clouds:
1. DTM/TIN generation via Delaunay triangulation
2. Breakline extraction from slope discontinuities
3. Contour generation (minor/index)
4. Hardscape & footprint extraction
"""

import time
import numpy as np
from typing import Optional, List, Dict, Any
from pathlib import Path

from totali.pipeline.models import (
    PhaseResult,
    ExtractionResult,
    ClassificationResult,
)
from totali.pipeline.base_phase import PipelinePhase


class DeterministicExtractor(PipelinePhase):
    def __init__(self, config: dict, audit: Any):
        super().__init__(config, audit)
        self.dtm_cfg = config.get("dtm", {})
        self.brk_cfg = config.get("breaklines", {})
        self.cnt_cfg = config.get("contours", {})
        self.plan_cfg = config.get("planimetrics", {})

    def validate_inputs(self, ctx: Any) -> tuple:
        errors = []
        if ctx.points_xyz is None:
            errors.append("points_xyz is required")
        if ctx.classification is None:
            errors.append("classification is required")
        return len(errors) == 0, errors

    def run(self, ctx: Any) -> PhaseResult:
        t0 = time.time()
        result = ExtractionResult()

        # 1. Filter ground points
        ground_mask = ctx.classification.labels == 2  # Standard LAS class for ground
        ground_pts = ctx.points_xyz[ground_mask]

        if len(ground_pts) < 3:
            return PhaseResult(
                phase="extract",
                success=False,
                message="Insufficient ground points for extraction",
            )

        # 2. Build DTM
        vertices, faces, dtm_metrics = self._build_dtm(ground_pts)
        result.dtm_vertices = vertices
        result.dtm_faces = faces
        result.error_metrics.update(dtm_metrics)

        # 3. Extract Breaklines
        breaklines, brk_metrics = self._extract_breaklines(ground_pts, vertices, faces)
        result.breaklines = breaklines
        result.error_metrics.update(brk_metrics)

        # 4. Generate Contours
        minor, index, cnt_metrics = self._generate_contours(vertices, faces)
        result.contours_minor = minor
        result.contours_index = index
        result.error_metrics.update(cnt_metrics)

        # 5. Planimetrics (Buildings, Curbs)
        # Simplified: just building footprints for now
        building_pts = ctx.points_xyz[ctx.classification.labels == 6]
        result.building_footprints = self._extract_building_footprints(building_pts)

        # 6. QA Flags
        result.qa_flags = self._generate_qa_flags(result, ctx.classification)

        # Persistence (simulated)
        output_files = [
            ctx.output_dir / f"{Path(ctx.input_path).stem}_extraction.pkl"
        ]

        return PhaseResult(
            phase="extract",
            success=True,
            data={"extraction": result},
            output_files=output_files,
            duration_sec=time.time() - t0,
        )

    def _build_dtm(self, ground_pts: np.ndarray) -> tuple:
        """Create a TIN (Triangulated Irregular Network) from ground points."""
        from scipy.spatial import Delaunay

        # 2D triangulation
        tri = Delaunay(ground_pts[:, :2])
        vertices = ground_pts
        faces = tri.simplices

        # Edge length filtering
        max_len = self.dtm_cfg.get("max_triangle_edge_length", 50.0)
        if max_len > 0:
            # Mask faces with any edge > max_len
            v0 = vertices[faces[:, 0]]
            v1 = vertices[faces[:, 1]]
            v2 = vertices[faces[:, 2]]

            e0 = np.linalg.norm(v1 - v0, axis=1)
            e1 = np.linalg.norm(v2 - v1, axis=1)
            e2 = np.linalg.norm(v0 - v2, axis=1)

            valid_mask = (e0 <= max_len) & (e1 <= max_len) & (e2 <= max_len)
            faces = faces[valid_mask]

            if len(faces) > 0:
                all_edges = np.concatenate([e0[valid_mask], e1[valid_mask], e2[valid_mask]])
                metrics = {
                    "vertex_count": len(vertices),
                    "face_count": len(faces),
                    "mean_edge_length": float(np.mean(all_edges)),
                    "max_edge_length": float(np.max(all_edges)),
                    "min_edge_length": float(np.min(all_edges)),
                }
            else:
                metrics = {"vertex_count": len(vertices), "face_count": 0}
        else:
            metrics = {"vertex_count": len(vertices), "face_count": len(faces)}

        return vertices, faces, metrics

    def _extract_breaklines(
        self, ground_pts: np.ndarray, vertices: np.ndarray, faces: np.ndarray
    ) -> tuple:
        """
        Extract breaklines from slope discontinuities in the TIN.
        Breaklines are edges where adjacent triangle slopes differ significantly.
        """
        min_angle = np.radians(self.brk_cfg.get("min_angle_degrees", 15.0))
        min_length = self.brk_cfg.get("min_length_ft", 5.0)
        breaklines = []

        if len(faces) < 2:
            return breaklines, {"count": 0}

        # Compute face normals (vectorized)
        v0 = vertices[faces[:, 0]]
        v1 = vertices[faces[:, 1]]
        v2 = vertices[faces[:, 2]]

        n = np.cross(v1 - v0, v2 - v0)
        norm = np.linalg.norm(n, axis=1, keepdims=True)
        normals = n / np.where(norm > 1e-10, norm, 1.0)
        # Default to vertical for degenerate triangles
        normals[norm.ravel() <= 1e-10] = [0, 0, 1]

        # Vectorized edge-to-face adjacency
        # Each face (v0, v1, v2) has edges (v0, v1), (v1, v2), (v2, v0)
        edges = np.vstack([
            np.sort(faces[:, [0, 1]], axis=1),
            np.sort(faces[:, [1, 2]], axis=1),
            np.sort(faces[:, [2, 0]], axis=1)
        ])
        face_indices = np.tile(np.arange(len(faces)), 3)

        # Lexicographical sort of edges by packing indices into int64
        # This is much faster than np.lexsort for large datasets
        edge_keys = edges[:, 0].astype(np.int64) << 32 | edges[:, 1].astype(np.int64)
        sort_idx = np.argsort(edge_keys)
        sorted_keys = edge_keys[sort_idx]
        sorted_faces = face_indices[sort_idx]

        # Find unique edges and their counts
        unique_keys, first_idx, counts = np.unique(sorted_keys, return_index=True, return_counts=True)

        # Identify edges shared by exactly two faces (potential breaklines)
        pair_mask = (counts == 2)
        pair_keys = unique_keys[pair_mask]
        p_idx = first_idx[pair_mask]

        # Adjacent face IDs
        f1 = sorted_faces[p_idx]
        f2 = sorted_faces[p_idx + 1]

        # Vectorized angle calculation
        dots = np.einsum('ij,ij->i', normals[f1], normals[f2])
        angles = np.arccos(np.clip(dots, -1, 1))

        # Vectorized edge length calculation
        v0_idx = (pair_keys >> 32).astype(np.int32)
        v1_idx = (pair_keys & 0xFFFFFFFF).astype(np.int32)
        edge_lens = np.linalg.norm(vertices[v0_idx] - vertices[v1_idx], axis=1)

        # Filter by threshold
        break_mask = (angles > min_angle) & (edge_lens >= min_length)

        indices = np.where(break_mask)[0]
        breakline_edges = [
            (int(v0_idx[i]), int(v1_idx[i]), float(angles[i]), float(edge_lens[i]))
            for i in indices
        ]

        # Chain connected edges into polylines
        if breakline_edges:
            chains = self._chain_edges(breakline_edges, vertices)
            breaklines = chains

        metrics = {
            "count": len(breaklines),
            "total_edges_checked": len(unique_keys),
            "breakline_edges_found": len(breakline_edges),
        }
        return breaklines, metrics

    def _chain_edges(self, edges: list, vertices: np.ndarray) -> list:
        """Chain connected breakline edges into polylines."""
        # Simplified: just return individual edge segments as 2-point lines
        chains = []
        for v0, v1, angle, length in edges:
            chains.append(np.array([vertices[v0], vertices[v1]]))
        return chains

    def _generate_contours(
        self, vertices: np.ndarray, faces: np.ndarray
    ) -> tuple:
        """Generate contour lines by intersecting TIN with horizontal planes."""
        interval = self.cnt_cfg.get("interval_ft", 1.0)
        index_interval = self.cnt_cfg.get("index_interval_ft", 5.0)

        if len(faces) == 0:
            return [], [], {"count": 0}

        z_min = vertices[:, 2].min()
        z_max = vertices[:, 2].max()

        minor_contours = []
        index_contours = []

        # Generate contour elevations
        z_start = np.ceil(z_min / interval) * interval
        elevations = np.arange(z_start, z_max, interval)

        for elev in elevations:
            segments = self._contour_at_elevation(vertices, faces, elev)
            if segments:
                if abs(elev % index_interval) < 0.01:
                    index_contours.extend(segments)
                else:
                    minor_contours.extend(segments)

        metrics = {
            "minor_count": len(minor_contours),
            "index_count": len(index_contours),
            "elevation_range": [float(z_min), float(z_max)],
            "interval": interval,
        }

        return minor_contours, index_contours, metrics

    def _contour_at_elevation(
        self, vertices: np.ndarray, faces: np.ndarray, elev: float
    ) -> list:
        """Extract contour line segments at a given elevation from TIN."""
        segments = []

        for face in faces:
            v = vertices[face]
            z = v[:, 2]

            # Find edges that cross this elevation
            crossings = []
            for i in range(3):
                j = (i + 1) % 3
                if (z[i] - elev) * (z[j] - elev) < 0:
                    # Linear interpolation
                    t = (elev - z[i]) / (z[j] - z[i])
                    pt = v[i] + t * (v[j] - v[i])
                    crossings.append(pt[:2])  # XY only for contour

            if len(crossings) == 2:
                segments.append(np.array(crossings))

        return segments

    def _extract_building_footprints(self, pts: np.ndarray) -> list:
        """Extract building footprints using alpha shapes / convex hulls."""
        min_area = self.plan_cfg.get("min_building_area_sqft", 100.0)

        try:
            from scipy.spatial import ConvexHull
        except ImportError:
            return []

        # Simple clustering by XY proximity
        clusters = self._cluster_points_2d(pts, radius=5.0)
        footprints = []

        for cluster_pts in clusters:
            if len(cluster_pts) < 4:
                continue
            try:
                hull = ConvexHull(cluster_pts[:, :2])
                if hull.volume >= min_area:
                    footprints.append(cluster_pts[hull.vertices][:, :2])
            except Exception:
                continue

        return footprints

    def _cluster_points_2d(self, pts: np.ndarray, radius: float) -> list:
        """Simple spatial clustering."""
        if len(pts) == 0:
            return []

        from scipy.spatial import KDTree
        tree = KDTree(pts[:, :2])
        visited = np.zeros(len(pts), dtype=bool)
        clusters = []

        for i in range(len(pts)):
            if not visited[i]:
                indices = tree.query_ball_point(pts[i, :2], radius)
                visited[indices] = True
                clusters.append(pts[indices])

        return clusters

    def _generate_qa_flags(self, result: ExtractionResult, classification: ClassificationResult) -> list:
        """Generate automated flags for human review."""
        flags = []

        # Flag low confidence areas
        if classification.low_confidence_count > 0:
            flags.append({
                "type": "low_confidence",
                "severity": "medium",
                "message": f"Found {classification.low_confidence_count} low-confidence points.",
            })

        # Flag occlusion zones
        for zone in result.occlusion_zones:
            flags.append({
                "type": "occlusion",
                "severity": "high",
                "message": "Potential canopy occlusion. Verify ground surface.",
                "geometry": zone.tolist(),
            })

        return flags
