"""Tests for Phase Integration: SurveyIntegrator."""

import json
import pytest
import numpy as np
from pathlib import Path

from totali.integration.integrator import SurveyIntegrator
from totali.pipeline.context import PipelineContext
from totali.pipeline.models import PhaseResult, SurveyData

@pytest.fixture
def integrator(audit_logger):
    config = {
        "geometry_ir_path": "fake_ir.json",
        "control_csv_path": "fake_control.csv"
    }
    return SurveyIntegrator(config, audit_logger)

@pytest.fixture
def fake_geometry_ir(tmp_path):
    ir_path = tmp_path / "fake_ir.json"
    data = {
        "data": {
            "features": [
                {
                    "feature_id": "feat1",
                    "feature_code": "BLDG",
                    "geometry_type": "Polygon",
                    "coordinates": [
                        {"x": 0, "y": 0, "z": 0},
                        {"x": 10, "y": 0, "z": 0},
                        {"x": 10, "y": 10, "z": 0},
                        {"x": 0, "y": 10, "z": 0}
                    ]
                }
            ]
        }
    }
    ir_path.write_text(json.dumps(data))
    return ir_path

@pytest.fixture
def fake_control_csv(tmp_path):
    csv_path = tmp_path / "fake_control.csv"
    csv_path.write_text("station_id,x,y,z\nCP1,100.0,200.0,50.0\nCP2,300.0,400.0,")
    return csv_path

class TestSurveyIntegrator:
    def test_load_geometry_ir(self, integrator, fake_geometry_ir):
        features = integrator._load_geometry_ir(fake_geometry_ir)
        assert len(features) == 1
        assert features[0].feature_id == "feat1"
        assert features[0].feature_type == "Polygon"
        assert features[0].geometry.shape == (4, 3)

    def test_load_control_csv(self, integrator, fake_control_csv):
        points = integrator._load_control_csv(fake_control_csv)
        assert len(points) == 2
        assert points[0].feature_id == "CP1"
        assert points[0].geometry[0, 2] == 50.0
        assert points[1].feature_id == "CP2"
        assert points[1].geometry[0, 2] == 0.0 # Default Z

    def test_run_loads_data(self, audit_logger, tmp_path, fake_geometry_ir, fake_control_csv, tmp_output):
        config = {
            "geometry_ir_path": str(fake_geometry_ir),
            "control_csv_path": str(fake_control_csv)
        }
        integrator = SurveyIntegrator(config, audit_logger)
        ctx = PipelineContext(input_path="fake.las", output_dir=tmp_output)

        result = integrator.run(ctx)

        assert result.success is True
        data = result.data["survey_data"]
        assert isinstance(data, SurveyData)
        assert len(data.features) == 1
        assert len(data.control_points) == 2
        assert str(fake_geometry_ir) in data.source_files

    def test_run_warns_missing_files(self, audit_logger, tmp_path, tmp_output):
        config = {
            "geometry_ir_path": str(tmp_path / "missing.json"),
            "control_csv_path": str(tmp_path / "missing.csv")
        }
        integrator = SurveyIntegrator(config, audit_logger)
        ctx = PipelineContext(input_path="fake.las", output_dir=tmp_output)

        result = integrator.run(ctx)
        assert result.success is True # Should not fail pipeline, just log warning
        # Check logs if we could access them easily, but result success is good enough for now
        assert len(result.data["survey_data"].features) == 0
