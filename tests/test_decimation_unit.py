import importlib.util
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest


DECIMATION_PATH = (
    Path(__file__).resolve().parents[1]
    / "groundtruthos-data"
    / "pipeline"
    / "decimation.py"
)

if not DECIMATION_PATH.exists():
    pytest.skip("groundtruthos-data/pipeline/decimation.py not present", allow_module_level=True)

mock_np = MagicMock()
mock_np.bool_ = bool

# Mock optional dependencies only while importing the module under test.
with patch.dict(
    sys.modules,
    {"laspy": MagicMock(), "numpy": mock_np, "psycopg2": MagicMock()},
):
    spec = importlib.util.spec_from_file_location("_decimation_under_test", DECIMATION_PATH)
    decimation = importlib.util.module_from_spec(spec)
    assert spec is not None and spec.loader is not None
    spec.loader.exec_module(decimation)

compute_adaptive_voxel_size = decimation.compute_adaptive_voxel_size
compute_density = decimation.compute_density
should_protect_region = decimation.should_protect_region
DEFAULT_BASE_VOXEL = decimation.DEFAULT_BASE_VOXEL
BREAKLINE_Z_STD_THRESHOLD = decimation.BREAKLINE_Z_STD_THRESHOLD


def test_numpy_module_restored_after_import():
    import numpy as np

    assert sys.modules["numpy"] is np
    assert sys.modules["numpy"] is not mock_np


def test_compute_adaptive_voxel_size():
    # current_density <= 0 or target_density <= 0
    assert compute_adaptive_voxel_size(0.0, 10.0, 0.5) == 0.5
    assert compute_adaptive_voxel_size(10.0, 0.0, 0.5) == 0.5
    assert compute_adaptive_voxel_size(-1.0, 10.0, 0.5) == 0.5

    # current_density <= target_density
    assert compute_adaptive_voxel_size(5.0, 10.0, 0.5) == 0.5
    assert compute_adaptive_voxel_size(10.0, 10.0, 0.5) == 0.5

    # current_density > target_density
    # ratio = 100 / 25 = 4; sqrt(4) = 2.0; 0.5 * 2.0 = 1.0
    assert compute_adaptive_voxel_size(100.0, 25.0, 0.5) == 1.0

    # default base voxel
    expected = DEFAULT_BASE_VOXEL * 2.0
    assert float(compute_adaptive_voxel_size(100.0, 25.0)) == pytest.approx(float(expected))


def test_compute_density():
    # Normal case: area = 100
    bounds = {"x_min": 0, "y_min": 0, "x_max": 10, "y_max": 10}
    assert compute_density(500, bounds) == 5.0

    # Area <= 0 case (x_max == x_min)
    bounds_zero = {"x_min": 10, "y_min": 0, "x_max": 10, "y_max": 10}
    assert compute_density(500, bounds_zero) == 0.0

    # Negative area
    bounds_neg = {"x_min": 20, "y_min": 0, "x_max": 10, "y_max": 10}
    assert compute_density(500, bounds_neg) == 0.0


def test_should_protect_region():
    # Above threshold
    assert should_protect_region(BREAKLINE_Z_STD_THRESHOLD + 0.1) is True
    # Below threshold
    assert should_protect_region(BREAKLINE_Z_STD_THRESHOLD - 0.1) is False
    # Exactly at threshold
    assert should_protect_region(BREAKLINE_Z_STD_THRESHOLD) is True

    # Custom threshold
    assert should_protect_region(5.0, threshold=4.0) is True
    assert should_protect_region(3.0, threshold=4.0) is False
