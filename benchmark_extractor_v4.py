import numpy as np
import time
from totali.audit.logger import AuditLogger
import tempfile
import sys
from unittest.mock import MagicMock

# Mock totali.pipeline to avoid circular imports during standalone benchmarking
sys.modules['totali.pipeline'] = MagicMock()
sys.modules['totali.pipeline.base_phase'] = MagicMock()
sys.modules['totali.pipeline.context'] = MagicMock()
sys.modules['totali.pipeline.models'] = MagicMock()

import totali.extraction.extractor as extractor_mod

def run_benchmark():
    # Setup
    config = {
        "dtm": {"max_triangle_edge_length": 50.0, "thin_factor": 1.0},
        "breaklines": {},
        "contours": {},
        "planimetrics": {}
    }

    with tempfile.TemporaryDirectory() as tmp_dir:
        audit = AuditLogger(log_dir=tmp_dir, project_id="bench")
        extractor = extractor_mod.DeterministicExtractor(config, audit)

        # Generate points
        n_points = 50000
        rng = np.random.default_rng(42)
        xs = rng.uniform(0, 1000, n_points)
        ys = rng.uniform(0, 1000, n_points)
        zz = rng.uniform(100, 110, n_points)
        pts = np.column_stack([xs, ys, zz])

        print(f"Running benchmark with {n_points} points...")

        # Measure
        start_time = time.perf_counter()
        result = extractor._build_dtm(pts)
        end_time = time.perf_counter()

        print(f"Result type: {type(result)}")
        print(f"Result length: {len(result)}")

        duration = end_time - start_time
        print(f"Duration: {duration:.4f} seconds")

if __name__ == "__main__":
    run_benchmark()
