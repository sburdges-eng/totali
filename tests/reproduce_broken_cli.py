import sys
import os
from pathlib import Path
from unittest.mock import MagicMock

# Ensure we can import totali
sys.path.insert(0, os.getcwd())

from totali.pipeline.orchestrator import PipelineOrchestrator
from totali.audit.logger import AuditLogger
from totali.pipeline.context import PipelineConfig

# Stub heavy dependencies directly in this script if conftest isn't loaded
# However, importing from totali.* might trigger imports.
# Let's try to mock minimal config

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

    print("Attempting to run 'segment' phase directly...")
    result = orch.run(str(input_file), phase="segment")

    if not result.success:
        print(f"FAILED as expected: {result.phases[0].message}")
        if "points_xyz missing" in result.phases[0].message:
             print("CONFIRMED: Missing context prevents split execution.")
        else:
             print("Failure reason unexpected but failed nonetheless.")
    else:
        print("UNEXPECTED SUCCESS (this should fail without geodetic input)")
        sys.exit(1)

if __name__ == "__main__":
    try:
        test_split_execution_fails()
    except Exception as e:
        # If we crash on imports, that's also failure but maybe setup issue
        print(f"Crashed: {e}")
