"""
Shared fixtures for TOTaLi pipeline tests.
"""

import sys
import types
from pathlib import Path
from unittest.mock import MagicMock

import numpy as np
import pytest

# ---------------------------------------------------------------------------
# Stub out heavy optional dependencies that may not be installed in the test
# environment (laspy, pyproj, ezdxf, onnxruntime).  We create lightweight
# fakes so `import laspy` etc. succeed when the test suite loads the totali
# package.  Tests that exercise those boundaries use explicit mocks.
# ---------------------------------------------------------------------------

def _ensure_stub(module_name: str, attrs: dict | None = None):
    """Register a stub module if the real one isn't importable."""
    try:
        __import__(module_name)
    except ImportError:
        mod = types.ModuleType(module_name)
        for k, v in (attrs or {}).items():
            setattr(mod, k, v)
        sys.modules[module_name] = mod


class _FakeCRS:
    """Minimal pyproj.CRS stand-in."""
    def __init__(self, *a, **kw):
        self.datum = MagicMock(name="NAD83(2011)")
    @classmethod
    def from_user_input(cls, s):
        inst = cls()
        inst._epsg = int(str(s).split(":")[-1]) if ":" in str(s) else 0
        return inst
    @classmethod
    def from_epsg(cls, code):
        inst = cls()
        inst._epsg = code
        return inst
    @classmethod
    def from_wkt(cls, wkt):
        inst = cls()
        inst._epsg = 2231
        return inst
    def to_epsg(self):
        return getattr(self, "_epsg", 0)


class _FakeTransformer:
    @classmethod
    def from_crs(cls, src, tgt, always_xy=True):
        return cls()
    def transform(self, x, y, z):
        return x, y, z


class _FakeLasHeader:
    def __init__(self, point_format=None, version=None):
        self.point_format = point_format or MagicMock(id=6)
        self.version = version
        self.offsets = np.array([0.0, 0.0, 0.0])
        self.scales = [0.001, 0.001, 0.001]
        self.point_count = 100
        self.vlrs = []


class _FakeLasData:
    """Minimal laspy.LasData stand-in backed by numpy arrays."""
    def __init__(self, header=None, n_points=100):
        self.header = header or _FakeLasHeader()
        self.header.point_count = n_points
        rng = np.random.default_rng(42)
        self._n = n_points
        self._x = rng.uniform(0, 1000, n_points)
        self._y = rng.uniform(0, 1000, n_points)
        self._z = rng.uniform(100, 200, n_points)
        self.classification = rng.choice([0, 2, 3, 5, 6], n_points)
        self.intensity = rng.integers(0, 65535, n_points, dtype=np.uint16)
        self.return_number = np.ones(n_points, dtype=np.uint8)
        self.number_of_returns = np.ones(n_points, dtype=np.uint8)
        self.vlrs = []

    @property
    def x(self): return self._x
    @x.setter
    def x(self, v): self._x = v
    @property
    def y(self): return self._y
    @y.setter
    def y(self, v): self._y = v
    @property
    def z(self): return self._z
    @z.setter
    def z(self, v): self._z = v
    @property
    def points(self): return np.arange(self._n)

    def write(self, path):
        pass


class _FakeLasReader:
    def __init__(self, path):
        self.path = path
        self.header = _FakeLasHeader()
    def __enter__(self):
        return self
    def __exit__(self, exc_type, exc_val, exc_tb):
        pass
    def read(self):
        return _FakeLasData(header=self.header)


class _FakeLaspyModule:
    LasData = _FakeLasData
    LasHeader = _FakeLasHeader

    @staticmethod
    def read(path):
        return _FakeLasData()

    @staticmethod
    def open(path, mode="r", header=None):
        return _FakeLasReader(path)


# Stub pyproj
_ensure_stub("pyproj", {
    "CRS": _FakeCRS,
    "Transformer": _FakeTransformer,
})
_ensure_stub("pyproj.exceptions", {"CRSError": type("CRSError", (Exception,), {})})

# Stub laspy
sys.modules.setdefault("laspy", _FakeLaspyModule)

# ezdxf is optional — stub it so `import ezdxf` succeeds at the module
# level (avoids circular-import issues during collection) but any call to
# ezdxf.new() or other real API methods triggers the fallback path.
# The shield code does `try: import ezdxf ... except ImportError`, so
# we need the *import* to succeed but usage to fail.  We patch the test
# targets individually (see test_shield.py) rather than making the stub
# raise, which would break the import guard pattern.
_ensure_stub("ezdxf")

# Stub onnxruntime (optional ML dep)
_ensure_stub("onnxruntime")


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def tmp_output(tmp_path):
    """Temporary output directory."""
    out = tmp_path / "output"
    out.mkdir()
    return out


@pytest.fixture
def audit_logger(tmp_path):
    """Fresh AuditLogger writing to a temp directory."""
    from totali.audit.logger import AuditLogger
    return AuditLogger(log_dir=str(tmp_path / "audit"), project_id="test_project")


@pytest.fixture
def sample_config():
    """Minimal pipeline config dict matching pipeline.yaml structure."""
    return {
        "project": {"name": "test_project", "version": "0.1.0"},
        "geodetic": {
            "allowed_crs": ["EPSG:2231"],
            "reject_on_missing_crs": True,
            "reject_on_mixed_datum: True": True,
            "geoid_model": "GEOID18",
            "elevation_unit": "US_survey_foot",
            "max_file_size_mb": 500,
            "max_point_count": 10000000,
        },
        "segmentation": {
            "model_path": "models/nonexistent.onnx",
            "device": "cpu",
            "confidence_threshold": 0.75,
            "occlusion_threshold": 0.30,
            "batch_size": 256,
            "voxel_size": 0.05,
            "classes": {0: "unclassified", 2: "ground", 6: "building"},
        },
        "extraction": {
            "dtm": {"max_triangle_edge_length": 50.0, "thin_factor": 1.0},
            "breaklines": {"min_angle_degrees": 15.0, "min_length_ft": 5.0},
            "contours": {"interval_ft": 1.0, "index_interval_ft": 5.0},
            "planimetrics": {"min_building_area_sqft": 100.0},
        },
        "cad_shielding": {
            "format": "dxf",
            "geometry_healing": {"close_tolerance": 0.001, "degenerate_face_threshold": 0.0001},
            "layer_mapping": {
                "ground_surface": "TOTaLi-SURV-DTM-DRAFT",
                "breaklines": "TOTaLi-SURV-BRKLN-DRAFT",
                "contours_minor": "TOTaLi-SURV-CONT-MINOR-DRAFT",
                "contours_index": "TOTaLi-SURV-CONT-INDEX-DRAFT",
                "buildings": "TOTaLi-PLAN-BLDG-DRAFT",
            },
            "middleware_timeout_sec": 10,
            "max_retry": 2,
        },
        "linting": {
            "ghost_opacity": 0.4,
            "auto_promote": False,
            "require_pls_signature": True,
        },
        "audit": {
            "log_dir": "audit_logs",
            "log_format": "jsonl",
            "hash_algorithm": "sha256",
        },
    }


@pytest.fixture
def sample_points():
    """Reproducible point cloud: 500 points with distinct elevation bands."""
    rng = np.random.default_rng(42)
    n = 500
    x = rng.uniform(100, 200, n)
    y = rng.uniform(100, 200, n)
    z = np.concatenate([
        rng.uniform(100, 105, 200),   # ground band
        rng.uniform(105, 115, 100),   # low/mid veg
        rng.uniform(115, 130, 100),   # high veg
        rng.uniform(130, 160, 100),   # buildings
    ])
    return np.column_stack([x, y, z])


@pytest.fixture
def sample_las():
    """Fake LasData object."""
    return _FakeLasData(n_points=500)


@pytest.fixture
def sample_classification():
    """Pre-built ClassificationResult for testing downstream phases."""
    from totali.pipeline.models import ClassificationResult
    rng = np.random.default_rng(42)
    n = 500
    labels = np.zeros(n, dtype=np.int32)
    labels[:200] = 2   # ground
    labels[200:300] = 5  # high veg
    labels[300:400] = 6  # building
    labels[400:] = 0     # unclassified
    confidences = rng.uniform(0.3, 0.95, n).astype(np.float32)
    occlusion_mask = confidences < 0.35
    return ClassificationResult(
        labels=labels,
        confidences=confidences,
        occlusion_mask=occlusion_mask,
        class_counts={"ground": 200, "high_vegetation": 100, "building": 100},
        mean_confidence=float(np.mean(confidences)),
        low_confidence_count=int(np.sum(confidences < 0.75)),
        occluded_count=int(np.sum(occlusion_mask)),
    )


@pytest.fixture
def pipeline_context(tmp_output, sample_points, sample_las, sample_classification):
    """Fully populated PipelineContext for testing mid/late phases."""
    from totali.pipeline.context import PipelineContext
    from totali.pipeline.models import CRSMetadata, PointCloudStats

    return PipelineContext(
        input_path="/fake/input.las",
        output_dir=tmp_output,
        points_xyz=sample_points,
        las=sample_las,
        crs=CRSMetadata(epsg_code=2231, is_valid=True),
        stats=PointCloudStats(point_count=500),
        input_hash="abc123",
        classification=sample_classification,
    )
