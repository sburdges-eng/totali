import sys
import os
import types
from pathlib import Path
from unittest.mock import MagicMock
import numpy as np

# Mock heavy deps BEFORE importing totali
module_names = ["laspy", "pyproj", "ezdxf", "onnxruntime"]
for name in module_names:
    if name not in sys.modules:
        m = types.ModuleType(name)
        sys.modules[name] = m

# Setup specific mocks needed for logic to pass
import laspy
laspy.read = MagicMock()
fake_las = MagicMock()
fake_las.vlrs = []
fake_las.x = np.zeros(10)
fake_las.y = np.zeros(10)
fake_las.z = np.zeros(10)
laspy.read.return_value = fake_las

import pyproj
pyproj.CRS = MagicMock()
pyproj.CRS.from_user_input.return_value.to_epsg.return_value = 2231
pyproj.Transformer = MagicMock()

# Ensure we can import totali
sys.path.insert(0, os.getcwd())

from totali.pipeline.orchestrator import PipelineOrchestrator
from totali.audit.logger import AuditLogger
from totali.pipeline.context import PipelineConfig

def test_split_execution_fails():
    output_dir = Path("tmp/repro_out")
    output_dir.mkdir(parents=True, exist_ok=True)

    config = {
        "project": {"name": "test"},
        "geodetic": {"allowed_crs": ["EPSG:2231"]},
        "segmentation": {},
        "extraction": {},
        "cad_shielding": {},
        "linting": {},
        "audit": {"log_dir": str(output_dir)}
    }

    audit = AuditLogger(log_dir=str(output_dir), project_id="test")
    orch = PipelineOrchestrator(config, audit, output_dir)

    input_file = output_dir / "input.las"
    input_file.write_bytes(b"fake")

    print(f"Running phase='segment'...")
    # This should fail validation because geodetic didn't run
    result = orch.run(str(input_file), phase="segment")

    if not result.success:
        print(f"Result success: {result.success}")
        # Check if any phase failed
        for p in result.phases:
            print(f"Phase {p.phase}: {p.message}")
            if "run geodetic phase first" in p.message:
                print("FAILURE CONFIRMED: Missing dependency.")
                return

    print("Test finished without expected failure message.")

if __name__ == "__main__":
    test_split_execution_fails()
