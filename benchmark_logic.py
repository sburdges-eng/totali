import numpy as np
import time
from scipy.spatial import Delaunay

def build_dtm_original(pts, max_edge):
    tri = Delaunay(pts[:, :2])
    valid_faces = []
    for simplex in tri.simplices:
        verts = pts[simplex]
        edges = [
            np.linalg.norm(verts[0] - verts[1]),
            np.linalg.norm(verts[1] - verts[2]),
            np.linalg.norm(verts[2] - verts[0]),
        ]
        if max(edges) <= max_edge:
            valid_faces.append(simplex)
    faces = np.array(valid_faces) if valid_faces else np.empty((0, 3), dtype=int)

    if len(faces) > 0:
        all_edges = []
        for f in faces:
            v = pts[f]
            all_edges.extend([
                np.linalg.norm(v[0] - v[1]),
                np.linalg.norm(v[1] - v[2]),
                np.linalg.norm(v[2] - v[0]),
            ])
        all_edges = np.array(all_edges)
        metrics = {
            "mean": np.mean(all_edges),
            "max": np.max(all_edges),
        }
    else:
        metrics = {}
    return faces, metrics

def build_dtm_optimized(pts, max_edge):
    tri = Delaunay(pts[:, :2])
    simplices = tri.simplices

    # Vectorized edge length calculation
    # tri_pts shape: (M, 3, 3) where M is number of triangles
    tri_pts = pts[simplices]

    # Edges: (M, 3)
    # Using np.linalg.norm with axis=2
    # Vertices of each triangle: v0, v1, v2
    v0 = tri_pts[:, 0, :]
    v1 = tri_pts[:, 1, :]
    v2 = tri_pts[:, 2, :]

    l1 = np.linalg.norm(v0 - v1, axis=1)
    l2 = np.linalg.norm(v1 - v2, axis=1)
    l3 = np.linalg.norm(v2 - v0, axis=1)

    max_edges = np.maximum(np.maximum(l1, l2), l3)
    mask = max_edges <= max_edge
    faces = simplices[mask]

    if len(faces) > 0:
        # Re-use already computed lengths for valid faces
        valid_l1 = l1[mask]
        valid_l2 = l2[mask]
        valid_l3 = l3[mask]
        # Equivalent to all_edges.extend(...)
        all_edges = np.column_stack([valid_l1, valid_l2, valid_l3]).ravel()

        metrics = {
            "mean": np.mean(all_edges),
            "max": np.max(all_edges),
        }
    else:
        metrics = {}
    return faces, metrics

def run_benchmark():
    n_points = 100000
    rng = np.random.default_rng(42)
    pts = rng.uniform(0, 1000, (n_points, 3))
    max_edge = 50.0

    print(f"Benchmarking with {n_points} points...")

    # Original
    start = time.perf_counter()
    faces_orig, metrics_orig = build_dtm_original(pts, max_edge)
    dur_orig = time.perf_counter() - start
    print(f"Original:  {dur_orig:.4f}s (Faces: {len(faces_orig)})")

    # Optimized
    start = time.perf_counter()
    faces_opt, metrics_opt = build_dtm_optimized(pts, max_edge)
    dur_opt = time.perf_counter() - start
    print(f"Optimized: {dur_opt:.4f}s (Faces: {len(faces_opt)})")

    # Verify
    np.testing.assert_array_equal(faces_orig, faces_opt)
    assert abs(metrics_orig['mean'] - metrics_opt['mean']) < 1e-10
    assert abs(metrics_orig['max'] - metrics_opt['max']) < 1e-10
    print("Verification passed!")
    print(f"Speedup: {dur_orig/dur_opt:.2f}x")

if __name__ == "__main__":
    run_benchmark()
