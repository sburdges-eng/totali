import sys
from pathlib import Path

# Add groundtruthos-data to path so `pipeline.decimation` is importable.
_groundtruthos_data_path = str(Path(__file__).parent.parent / "groundtruthos-data")
if _groundtruthos_data_path not in sys.path:
    sys.path.insert(0, _groundtruthos_data_path)

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
