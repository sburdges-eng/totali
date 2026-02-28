"""
Phase 3: Deterministic Extraction
=================================
Builds a Digital Terrain Model (TIN) from classified ground points and
extracts secondary geometric artifacts (contours, breaklines, planimetrics).
"""

import logging
import pickle
from pathlib import Path
from typing import Any

import numpy as np
from scipy.spatial import Delaunay

from totali.audit.logger import AuditLogger
from totali.pipeline.base_phase import PipelinePhase
from totali.pipeline.context import PipelineContext
from totali.pipeline.models import PhaseResult, ExtractionResult, ClassificationResult


class DeterministicExtractor(PipelinePhase):
    def __init__(self, config: dict, audit: AuditLogger):
        super().__init__(config, audit)
        self.dtm_cfg = self.config.get("dtm", {})
        self.brk_cfg = self.config.get("breaklines", {})
        self.cnt_cfg = self.config.get("contours", {})
        self.plan_cfg = self.config.get("planimetrics", {})

    def validate_inputs(self, ctx: PipelineContext) -> tuple[bool, list[str]]:
        errors = []
        if ctx.points_xyz is None:
            errors.append("points_xyz missing from context")
        if ctx.classification is None:
            errors.append("classification result missing from context")
        return len(errors) == 0, errors

    def run(self, ctx: PipelineContext) -> PhaseResult:
        self.audit.log("extraction_start", {"input": ctx.input_path})

        # 1. Filter ground points
        ground_mask = ctx.classification.labels == 2  # ASPRS standard for Ground
        ground_pts = ctx.points_xyz[ground_mask]

        if len(ground_pts) < 3:
            return PhaseResult(
                phase="extract",
                success=False,
                message=f"Insufficient ground points ({len(ground_pts)}) for triangulation",
            )

        # 2. Build DTM (TIN)
        vertices, faces, dtm_metrics = self._build_dtm(ground_pts)

        # 3. Extract Breaklines
        breaklines, brk_metrics = self._extract_breaklines(ground_pts, vertices, faces)

        # 4. Generate Contours
        minor_cnt, index_cnt, cnt_metrics = self._generate_contours(vertices, faces)

        # 5. Extract Planimetrics (Buildings, Hardscape)
        buildings = self._extract_building_footprints(
            ctx.points_xyz[ctx.classification.labels == 6]
        )

        # 6. Accumulate results
        result = ExtractionResult(
            dtm_vertices=vertices,
            dtm_faces=faces,
            breaklines=breaklines,
            contours_minor=minor_cnt,
            contours_index=index_cnt,
            building_footprints=buildings,
            occlusion_zones=self._build_occlusion_zones(
                ctx.points_xyz[ctx.classification.occlusion_mask]
            ),
            error_metrics={"dtm": dtm_metrics, "breaklines": brk_metrics, "contours": cnt_metrics},
        )

        # 7. Generate QA Flags
        result.qa_flags = self._generate_qa_flags(result, ctx.classification)

        return PhaseResult(
            phase="extract",
            success=True,
            data={"extraction": result},
            output_files=[self._save_extraction(result, ctx)],
        )

    def _save_extraction(self, result: ExtractionResult, ctx: PipelineContext) -> Path:
        path = Path(ctx.output_dir) / f"{Path(ctx.input_path).stem}_extraction.pkl"
        with open(path, "wb") as f:
            pickle.dump(result, f)
        return path

    def _build_dtm(self, pts: np.ndarray) -> tuple:
        """Build Delaunay Triangulation and prune long edges."""
        max_edge = self.dtm_cfg.get("max_triangle_edge_length", 50.0)

        # 2D triangulation (XY)
        tri = Delaunay(pts[:, :2])
        faces = tri.simplices

        # Filter faces by edge length to avoid interpolation over large gaps
        v0 = pts[faces[:, 0]]
        v1 = pts[faces[:, 1]]
        v2 = pts[faces[:, 2]]

        e0 = np.linalg.norm(v1 - v0, axis=1)
        e1 = np.linalg.norm(v2 - v1, axis=1)
        e2 = np.linalg.norm(v0 - v2, axis=1)

        valid_mask = (e0 <= max_edge) & (e1 <= max_edge) & (e2 <= max_edge)
        faces = faces[valid_mask]

        if len(faces) > 0:
            all_edges = np.concatenate([e0[valid_mask], e1[valid_mask], e2[valid_mask]])
            metrics = {
                "vertex_count": len(pts),
                "face_count": len(faces),
                "max_edge_length": float(np.max(all_edges)),
                "min_edge_length": float(np.min(all_edges)),
            }
        else:
            metrics = {"vertex_count": len(pts), "face_count": 0}

        return pts, faces, metrics

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

        # Vectorized face normal computation
        v0 = vertices[faces[:, 0]]
        v1 = vertices[faces[:, 1]]
        v2 = vertices[faces[:, 2]]

        raw_normals = np.cross(v1 - v0, v2 - v0)
        norms = np.linalg.norm(raw_normals, axis=1, keepdims=True)

        normals = np.zeros_like(raw_normals)
        mask = (norms > 1e-10).flatten()
        normals[mask] = raw_normals[mask] / norms[mask]
        normals[~mask] = [0.0, 0.0, 1.0]

        # Vectorized edge-to-face adjacency
        # Extract all edges and sort vertices within each edge
        all_edges = np.concatenate([
            faces[:, [0, 1]],
            faces[:, [1, 2]],
            faces[:, [2, 0]]
        ], axis=0)
        all_edges.sort(axis=1)

        # Keep track of which face each edge came from
        edge_to_face = np.tile(np.arange(len(faces)), 3)

        # Sort edges lexicographically to bring duplicates together
        order = np.lexsort((all_edges[:, 1], all_edges[:, 0]))
        sorted_edges = all_edges[order]
        sorted_faces = edge_to_face[order]

        # Find where new edges start in the sorted list
        edge_diff = np.any(sorted_edges[1:] != sorted_edges[:-1], axis=1)
        edge_starts = np.concatenate(([0], np.where(edge_diff)[0] + 1))
        edge_counts = np.diff(np.concatenate((edge_starts, [len(sorted_edges)])))

        # Only process edges shared by exactly two faces (internal edges)
        pair_indices = np.where(edge_counts == 2)[0]
        if pair_indices.size == 0:
            return [], {"count": 0, "total_edges_checked": len(edge_starts), "breakline_edges_found": 0}

        pair_starts = edge_starts[pair_indices]

        f0 = sorted_faces[pair_starts]
        f1 = sorted_faces[pair_starts + 1]

        # Compute angles between adjacent face normals
        dot_products = np.sum(normals[f0] * normals[f1], axis=1)
        angles = np.arccos(np.clip(dot_products, -1.0, 1.0))

        # Compute edge lengths
        v0_idx = sorted_edges[pair_starts, 0]
        v1_idx = sorted_edges[pair_starts, 1]
        edge_lens = np.linalg.norm(vertices[v1_idx] - vertices[v0_idx], axis=1)

        # Filter by threshold
        breakline_mask = (angles > min_angle) & (edge_lens >= min_length)

        breakline_edges = []
        for i in np.where(breakline_mask)[0]:
            breakline_edges.append((
                int(v0_idx[i]),
                int(v1_idx[i]),
                float(angles[i]),
                float(edge_lens[i])
            ))

        # Chain connected edges into polylines
        if breakline_edges:
            chains = self._chain_edges(breakline_edges, vertices)
            breaklines = chains

        metrics = {
            "count": len(breaklines),
            "total_edges_checked": len(edge_starts),
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
                area = hull.volume  # 2D ConvexHull.volume = area
                if area >= min_area:
                    hull_pts = cluster_pts[hull.vertices, :2]
                    footprints.append(hull_pts)
            except Exception:
                continue

        return footprints

    def _extract_linear_features(self, pts: np.ndarray, feature_type: str) -> list:
        """Extract linear features (curbs, wires) by ordering points along principal axis."""
        if len(pts) < 2:
            return []

        clusters = self._cluster_points_2d(pts, radius=3.0)
        lines = []

        for cluster_pts in clusters:
            if len(cluster_pts) < 2:
                continue
            # PCA to find principal direction, then sort along it
            mean = cluster_pts.mean(axis=0)
            centered = cluster_pts - mean
            cov = np.cov(centered[:, :2].T)
            eigenvalues, eigenvectors = np.linalg.eigh(cov)
            principal = eigenvectors[:, -1]
            projections = centered[:, :2] @ principal
            order = np.argsort(projections)
            lines.append(cluster_pts[order])

        return lines

    def _extract_polygonal_features(self, pts: np.ndarray) -> list:
        """Extract polygonal features (hardscape) via convex hull clustering."""
        clusters = self._cluster_points_2d(pts, radius=3.0)
        polygons = []

        for cluster_pts in clusters:
            if len(cluster_pts) < 4:
                continue
            try:
                from scipy.spatial import ConvexHull
                hull = ConvexHull(cluster_pts[:, :2])
                hull_pts = cluster_pts[hull.vertices, :2]
                polygons.append(hull_pts)
            except Exception:
                continue

        return polygons

    def _build_occlusion_zones(self, occluded_pts: np.ndarray) -> list:
        """Build occlusion zone polygons from occluded points."""
        clusters = self._cluster_points_2d(occluded_pts, radius=10.0)
        zones = []

        for cluster_pts in clusters:
            if len(cluster_pts) < 3:
                continue
            try:
                from scipy.spatial import ConvexHull
                hull = ConvexHull(cluster_pts[:, :2])
                hull_pts = cluster_pts[hull.vertices, :2]
                zones.append(hull_pts)
            except Exception:
                continue

        return zones

    def _cluster_points_2d(self, pts: np.ndarray, radius: float) -> list:
        """Simple grid-based clustering."""
        if len(pts) == 0:
            return []

        xy = pts[:, :2]
        grid_size = radius * 2
        grid_keys = np.floor(xy / grid_size).astype(int)

        clusters_dict = {}
        for i, key in enumerate(grid_keys):
            k = tuple(key)
            clusters_dict.setdefault(k, []).append(i)

        # Merge adjacent grid cells
        clusters = []
        for indices in clusters_dict.values():
            if len(indices) >= 2:
                clusters.append(pts[indices])

        return clusters

    def _generate_qa_flags(
        self, result: ExtractionResult, classification: ClassificationResult
    ) -> list:
        """Generate QA flags for human review."""
        flags = []

        # Flag low-confidence areas
        if classification.low_confidence_count > 0:
            pct = classification.low_confidence_count / len(classification.labels)
            flags.append({
                "type": "low_confidence",
                "severity": "warning" if pct < 0.1 else "critical",
                "message": f"{classification.low_confidence_count} points "
                           f"({pct:.1%}) below confidence threshold",
                "count": classification.low_confidence_count,
            })

        # Flag occlusion zones
        if result.occlusion_zones:
            flags.append({
                "type": "occlusion",
                "severity": "info",
                "message": f"{len(result.occlusion_zones)} occlusion zones detected – "
                           "field verification recommended",
                "count": len(result.occlusion_zones),
            })

        # Flag thin DTM areas
        dtm_metrics = result.error_metrics.get("dtm", {})
        if dtm_metrics.get("max_edge_length", 0) > 30:
            flags.append({
                "type": "sparse_dtm",
                "severity": "warning",
                "message": f"Large DTM triangles detected "
                           f"(max edge: {dtm_metrics['max_edge_length']:.1f} ft)",
            })

        return flags
