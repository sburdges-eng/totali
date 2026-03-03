import sys
from unittest.mock import MagicMock

# Mock dependencies strictly within the test file to avoid crashing during import
# in environments without numpy, laspy, or psycopg2.
# This keeps the "hack" contained and avoids modifying production code structure.
sys.modules["laspy"] = MagicMock()
mock_np = MagicMock()
mock_np.bool_ = bool
sys.modules["numpy"] = mock_np
sys.modules["psycopg2"] = MagicMock()

import pytest
from pipeline.decimation import (
    compute_adaptive_voxel_size,
    compute_density,
    should_protect_region,
    DEFAULT_BASE_VOXEL,
    BREAKLINE_Z_STD_THRESHOLD
)

def test_compute_adaptive_voxel_size():
    # Test with current_density <= 0 or target_density <= 0
    assert compute_adaptive_voxel_size(0.0, 10.0, 0.5) == 0.5
    assert compute_adaptive_voxel_size(10.0, 0.0, 0.5) == 0.5
    assert compute_adaptive_voxel_size(-1.0, 10.0, 0.5) == 0.5

    # Test with current_density <= target_density
    assert compute_adaptive_voxel_size(5.0, 10.0, 0.5) == 0.5
    assert compute_adaptive_voxel_size(10.0, 10.0, 0.5) == 0.5

    # Test with current_density > target_density
    # ratio = 100 / 25 = 4; sqrt(4) = 2.0; 0.5 * 2.0 = 1.0
    assert compute_adaptive_voxel_size(100.0, 25.0, 0.5) == 1.0

    # Test with default base_voxel
    # ratio = 100 / 25 = 4; sqrt(4) = 2.0; DEFAULT_BASE_VOXEL * 2.0
    expected = DEFAULT_BASE_VOXEL * 2.0
    assert float(compute_adaptive_voxel_size(100.0, 25.0)) == pytest.approx(float(expected))

def test_compute_density():
    # Normal case
    bounds = {"x_min": 0, "y_min": 0, "x_max": 10, "y_max": 10}
    # Area = 100
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
