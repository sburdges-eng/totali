"""
Phase 3: Deterministic Extraction
==================================
Authoritative geometry generation from classified points.
Uses deterministic algorithms only – no generative AI, no hallucinated surfaces.
Produces measurable error metrics and QA flags.
"""

import json
from pathlib import Path
from typing import Optional

import numpy as np
from scipy.spatial import Delaunay, ConvexHull, QhullError
from scipy.ndimage import uniform_filter1d

from totali.pipeline.models import (
    PhaseResult, ExtractionResult, ClassificationResult
)
from totali.pipeline.base_phase import PipelinePhase
from totali.pipeline.context import PipelineContext
from totali.audit.logger import AuditLogger


class DeterministicExtractor(PipelinePhase):
    def __init__(self, config: dict, audit: AuditLogger):
        super().__init__(config, audit)
        self.dtm_cfg = config.get("dtm", {})
        self.brk_cfg = config.get("breaklines", {})
        self.cnt_cfg = config.get("contours", {})
        self.plan_cfg = config.get("planimetrics", {})

    def validate_inputs(self, context: PipelineContext) -> tuple[bool, list[str]]:
        errors: list[str] = []
        if context.points_xyz is None:
            errors.append("points_xyz missing; run geodetic phase first")
        if context.classification is None:
            errors.append("classification missing; run segment phase first")
        return len(errors) == 0, errors

    def run(self, context: PipelineContext) -> PhaseResult:
        xyz = context.points_xyz
        classification: ClassificationResult | None = context.classification
        output_dir = Path(context.output_dir)

        if xyz is None or classification is None:
            return PhaseResult(
                phase="extract", success=False,
                message="Missing point data or classification"
            )

        # 1. Coordinate alignment (if needed)
        # 2. Extract DTM (Ground points only)
        ground_mask = classification.labels == 2  # ASPRS Standard Class 2
        ground_pts = xyz[ground_mask]

        if len(ground_pts) < 100:
            return PhaseResult(
                phase="extract", success=False,
                message=f"Insufficient ground points ({len(ground_pts)}) for DTM generation"
            )

        dtm_vertices, dtm_faces, dtm_metrics = self._build_dtm(ground_pts)

        # 3. Extract Breaklines
        breaklines, brk_metrics = self._extract_breaklines(ground_pts, dtm_vertices, dtm_faces)

        # 4. Generate Contours
        minor, index, cnt_metrics = self._generate_contours(dtm_vertices, dtm_faces)

        # 5. Extract Planimetrics (Buildings, etc)
        building_mask = classification.labels == 6  # ASPRS Class 6
        curb_mask = classification.labels == 11
        wire_mask = classification.labels == 14
        hardscape_mask = classification.labels == 17

        result = ExtractionResult(
            dtm_vertices=dtm_vertices,
            dtm_faces=dtm_faces,
            breaklines=breaklines,
            contours_minor=minor,
            contours_index=index,
            error_metrics={
                "dtm": dtm_metrics,
                "breaklines": brk_metrics,
                "contours": cnt_metrics,
            }
        )

        if building_mask.any():
            result.building_footprints = self._extract_building_footprints(xyz[building_mask])
        if curb_mask.any():
            result.curb_lines = self._extract_linear_features(xyz[curb_mask], "curb")
        if wire_mask.any():
            result.wire_lines = self._extract_linear_features(xyz[wire_mask], "wire")
        if hardscape_mask.any():
            result.hardscape_polygons = self._extract_polygonal_features(xyz[hardscape_mask])

        # 5. Occlusion zones
        if classification.occlusion_mask is not None:
            occluded = xyz[classification.occlusion_mask]
            if len(occluded) > 0:
                result.occlusion_zones = self._build_occlusion_zones(occluded)

        # 6. QA flags
        result.qa_flags = self._generate_qa_flags(result, classification)

        # Write extraction report
        report_path = output_dir / "extraction_report.json"
        report = {
            "dtm_vertices": len(result.dtm_vertices) if result.dtm_vertices is not None else 0,
            "dtm_faces": len(result.dtm_faces) if result.dtm_faces is not None else 0,
            "breaklines": len(result.breaklines),
            "contours_minor": len(result.contours_minor),
            "contours_index": len(result.contours_index),
            "building_footprints": len(result.building_footprints),
            "occlusion_zones": len(result.occlusion_zones),
            "metrics": result.error_metrics,
        }
        with open(report_path, "w") as f:
            json.dump(report, f, indent=2)

        return PhaseResult(
            phase="extract",
            success=True,
            data={"extraction": result},
            output_files=[report_path]
        )

    def _build_dtm(self, ground_pts: np.ndarray) -> tuple:
        """Construct Digital Terrain Model (TIN) via Delaunay triangulation."""
        max_edge = self.dtm_cfg.get("max_triangle_edge_length", 50.0)

        # Delaunay on XY
        tri = Delaunay(ground_pts[:, :2])
        faces = tri.simplices
        pts = ground_pts

        # Filter large triangles (typically on bounds)
        filtered_faces = []
        all_edges = []
        for f in faces:
            v = pts[f]
            e1 = np.linalg.norm(v[0] - v[1])
            e2 = np.linalg.norm(v[1] - v[2])
            e3 = np.linalg.norm(v[2] - v[0])
            all_edges.extend([e1, e2, e3])
            if e1 <= max_edge and e2 <= max_edge and e3 <= max_edge:
                filtered_faces.append(f)

        faces = np.array(filtered_faces)
        if len(all_edges) > 0:
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

        # Compute face normals
        normals = np.zeros((len(faces), 3))
        for i, f in enumerate(faces):
            v0, v1, v2 = vertices[f[0]], vertices[f[1]], vertices[f[2]]
            n = np.cross(v1 - v0, v2 - v0)
            norm = np.linalg.norm(n)
            normals[i] = n / norm if norm > 1e-10 else [0, 0, 1]

        # Build edge-to-face adjacency
        edge_faces = {}
        for fi, f in enumerate(faces):
            for e in [(f[0], f[1]), (f[1], f[2]), (f[2], f[0])]:
                key = tuple(sorted(e))
                edge_faces.setdefault(key, []).append(fi)

        # Find breakline edges: adjacent faces with significant slope change
        breakline_edges = []
        for (v0, v1), face_ids in edge_faces.items():
            if len(face_ids) == 2:
                angle = np.arccos(
                    np.clip(np.dot(normals[face_ids[0]], normals[face_ids[1]]), -1, 1)
                )
                edge_len = np.linalg.norm(vertices[v0] - vertices[v1])
                if angle > min_angle and edge_len >= min_length:
                    breakline_edges.append((v0, v1, angle, edge_len))

        # Chain connected edges into polylines
        if breakline_edges:
            chains = self._chain_edges(breakline_edges, vertices)
            breaklines = chains

        metrics = {
            "count": len(breaklines),
            "total_edges_checked": len(edge_faces),
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
            except QhullError as e:
                self.audit.log("qhull_error", {
                    "method": "_extract_building_footprints",
                    "error": str(e),
                    "cluster_size": len(cluster_pts)
                })
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
                hull = ConvexHull(cluster_pts[:, :2])
                hull_pts = cluster_pts[hull.vertices, :2]
                polygons.append(hull_pts)
            except QhullError as e:
                self.audit.log("qhull_error", {
                    "method": "_extract_polygonal_features",
                    "error": str(e),
                    "cluster_size": len(cluster_pts)
                })
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
                hull = ConvexHull(cluster_pts[:, :2])
                hull_pts = cluster_pts[hull.vertices, :2]
                zones.append(hull_pts)
            except QhullError as e:
                self.audit.log("qhull_error", {
                    "method": "_build_occlusion_zones",
                    "error": str(e),
                    "cluster_size": len(cluster_pts)
                })
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
