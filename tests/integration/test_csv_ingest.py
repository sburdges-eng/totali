import pytest
import csv
from pathlib import Path
from totali.pipeline.orchestrator import PipelineOrchestrator
from totali.audit.logger import AuditLogger

@pytest.fixture
def csv_input(tmp_path):
    """Create a sample CSV file."""
    csv_path = tmp_path / "points.csv"
    with open(csv_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["point_id", "northing", "easting", "elevation", "description"])
        writer.writerow(["1", "1000.0", "5000.0", "100.0", "CP"])
        writer.writerow(["2", "1010.0", "5010.0", "105.0", "GND"])
        writer.writerow(["3", "1020.0", "5020.0", "110.0", "GND"])
    return csv_path

@pytest.fixture
def sample_config():
    """Minimal pipeline config dict."""
    return {
        "project": {"name": "test_project", "version": "0.1.0"},
        "geodetic": {
            "allowed_crs": ["EPSG:2231"],
            "reject_on_missing_crs": False,
            "geoid_model": "GEOID18",
            "elevation_unit": "US_survey_foot",
        },
        "segmentation": {
            "model_path": "models/nonexistent.onnx",
            "device": "cpu",
            "confidence_threshold": 0.75,
            "occlusion_threshold": 0.30,
            "batch_size": 256,
            "voxel_size": 0.05,
            "classes": {0: "unclassified", 2: "ground"},
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
            "layer_mapping": {},
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

def test_csv_ingest_succeeds(csv_input, tmp_path, sample_config):
    """Verify that CSV input is correctly ingested."""
    audit = AuditLogger(log_dir=str(tmp_path / "audit"), project_id="test")
    output_dir = tmp_path / "output"
    output_dir.mkdir()

    pipeline = PipelineOrchestrator(sample_config, audit, output_dir)

    result = pipeline.run(str(csv_input), phase="geodetic")

    assert result.success is True
    assert len(result.output_files) == 2  # .las and .json report

    # Check that points were read correctly (Gatekeeper returns points_xyz in data)
    points = result.phases[0].data.get("points_xyz")
    assert points is not None
    assert len(points) == 3
    # Check coordinate mapping (x=easting, y=northing)
    # Point 1: E=5000, N=1000
    assert points[0][0] == 5000.0
    assert points[0][1] == 1000.0

def test_audit_log_captures_ingest(csv_input, tmp_path, sample_config):
    """Verify audit log captures ingest event for CSV."""
    audit_dir = tmp_path / "audit"
    audit = AuditLogger(log_dir=str(audit_dir), project_id="test")
    output_dir = tmp_path / "output"
    output_dir.mkdir()

    pipeline = PipelineOrchestrator(sample_config, audit, output_dir)
    pipeline.run(str(csv_input), phase="geodetic")

    events = audit.get_events("ingest")
    assert len(events) == 1
    assert events[0]["data"]["file"] == str(csv_input)
    assert events[0]["data"]["point_count"] == 3
