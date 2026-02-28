import numpy as np
import time

def benchmark():
    print("--- Starting Face Normal Benchmark ---")
    # Setup data - 500k faces is a decent stress test for lidar data
    num_vertices = 250000
    num_faces = 500000
    vertices = np.random.rand(num_vertices, 3).astype(np.float64)
    faces = np.random.randint(0, num_vertices, (num_faces, 3))

    # Baseline: Current loop implementation
    print(f"Benchmarking Loop Implementation with {num_faces} faces...")
    start_time = time.time()
    normals_loop = np.zeros((len(faces), 3))
    for i, f in enumerate(faces):
        v0, v1, v2 = vertices[f[0]], vertices[f[1]], vertices[f[2]]
        n = np.cross(v1 - v0, v2 - v0)
        norm = np.linalg.norm(n)
        normals_loop[i] = n / norm if norm > 1e-10 else [0, 0, 1]
    end_time = time.time()
    duration_loop = end_time - start_time
    print(f"Loop duration: {duration_loop:.4f} seconds")

    # Vectorized: Proposed implementation
    print(f"Benchmarking Vectorized Implementation with {num_faces} faces...")
    start_time = time.time()

    # Proposed vectorized code
    v0 = vertices[faces[:, 0]]
    v1 = vertices[faces[:, 1]]
    v2 = vertices[faces[:, 2]]
    n_vec = np.cross(v1 - v0, v2 - v0)
    # Norm along axis 1 (the 3D vector dimension)
    norms = np.linalg.norm(n_vec, axis=1, keepdims=True)

    # Handle zero/small norms to avoid div by zero
    # We want [0, 0, 1] for norm <= 1e-10
    # Option A: np.where (cleaner)
    normals_vectorized = np.where(norms > 1e-10, n_vec / (norms + 1e-20), np.array([0, 0, 1.]))

    end_time = time.time()
    duration_vec = end_time - start_time
    print(f"Vectorized duration: {duration_vec:.4f} seconds")

    speedup = duration_loop / duration_vec
    print(f"Speedup: {speedup:.2f}x")

    # Verify correctness
    # Using atol because float precision might differ slightly due to ops order
    np.testing.assert_allclose(normals_loop, normals_vectorized, atol=1e-12)
    print("Correctness verified: Loop and Vectorized results match.")

    return duration_loop, duration_vec, speedup

if __name__ == "__main__":
    benchmark()
