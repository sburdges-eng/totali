"""Shared fixtures for TOTaLi tests."""
import yaml
import numpy as np
import laspy
import pytest
from pathlib import Path
from pyproj import CRS

from totali.audit.logger import AuditLogger
from totali.pipeline.models import ClassificationResult, ExtractionResult


@pytest.fixture
def config():
    config_path = Path(__file__).parent.parent / "config" / "pipeline.yaml"
    with open(config_path) as f:
        return yaml.safe_load(f)


@pytest.fixture
def audit(tmp_path):
    return AuditLogger(log_dir=str(tmp_path), project_id="unit_test")


@pytest.fixture
def output_dir(tmp_path):
    out = tmp_path / "output"
    out.mkdir()
    return out


@pytest.fixture
def synthetic_las_path():
    p = Path(__file__).parent / "synthetic_site.las"
    if not p.exists():
        from tests.generate_synthetic_las import generate_synthetic_las
        generate_synthetic_las(str(p))
    return str(p)


@pytest.fixture
def sample_xyz():
    """Small 100-point ground surface for unit tests."""
    rng = np.random.default_rng(42)
    x = rng.uniform(0, 100, 100)
    y = rng.uniform(0, 100, 100)
    z = 6100 + 0.01 * x + rng.normal(0, 0.05, 100)
    return np.column_stack([x, y, z])


@pytest.fixture
def sample_classification(sample_xyz):
    n = len(sample_xyz)
    labels = np.full(n, 2, dtype=np.int32)  # all ground
    confidences = np.full(n, 0.85, dtype=np.float32)
    return ClassificationResult(
        labels=labels,
        confidences=confidences,
        occlusion_mask=np.zeros(n, dtype=bool),
        mean_confidence=0.85,
        low_confidence_count=0,
        occluded_count=0,
    )


@pytest.fixture
def sample_extraction(sample_xyz):
    """Minimal extraction result for shield/lint testing."""
    from scipy.spatial import Delaunay
    tri = Delaunay(sample_xyz[:, :2])
    result = ExtractionResult()
    result.dtm_vertices = sample_xyz
    result.dtm_faces = tri.simplices
    result.breaklines = [np.array([[0, 0, 6100], [50, 50, 6100.5]])]
    result.contours_minor = [np.array([[10, 10], [20, 20]])]
    result.contours_index = [np.array([[30, 30], [40, 40]])]
    result.qa_flags = []
    return result
