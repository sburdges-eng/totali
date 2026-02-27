"""Tests for Phase 1: GeodeticGatekeeper."""

import hashlib
from pathlib import Path
from unittest.mock import patch, MagicMock

import numpy as np
import pytest

from totali.geodetic.gatekeeper import GeodeticGatekeeper
from totali.pipeline.context import PipelineContext
from totali.pipeline.models import PhaseResult, CRSMetadata


@pytest.fixture
def gatekeeper(audit_logger, sample_config):
    return GeodeticGatekeeper(sample_config["geodetic"], audit_logger)


class TestValidateInputs:
    def test_missing_input_path(self, gatekeeper, tmp_output):
        ctx = PipelineContext(input_path="", output_dir=tmp_output)
        valid, errors = gatekeeper.validate_inputs(ctx)
        assert valid is False
        assert any("input_path" in e for e in errors)

    def test_nonexistent_file(self, gatekeeper, tmp_output):
        ctx = PipelineContext(input_path="/no/such/file.las", output_dir=tmp_output)
        valid, errors = gatekeeper.validate_inputs(ctx)
        assert valid is False
        assert any("does not exist" in e for e in errors)

    def test_valid_input(self, gatekeeper, tmp_output, tmp_path):
        fake_las = tmp_path / "test.las"
        fake_las.write_bytes(b"\x00" * 100)
        ctx = PipelineContext(input_path=str(fake_las), output_dir=tmp_output)
        valid, errors = gatekeeper.validate_inputs(ctx)
        assert valid is True
        assert errors == []

    def test_empty_allowed_crs_rejected(self, audit_logger, tmp_output, tmp_path):
        """Empty allowed_crs must be rejected to avoid IndexError in run()."""
        config = {
            "allowed_crs": [],
            "reject_on_missing_crs": True,
            "geoid_model": "GEOID18",
            "elevation_unit": "US_survey_foot",
        }
        gk = GeodeticGatekeeper(config, audit_logger)
        fake = tmp_path / "test.las"
        fake.write_bytes(b"\x00" * 100)
        ctx = PipelineContext(input_path=str(fake), output_dir=tmp_output)
        valid, errors = gk.validate_inputs(ctx)
        assert valid is False
        assert any("allowed_crs" in e for e in errors)


class TestCRSExtraction:
    def test_no_vlrs_rejects_when_configured(self, gatekeeper):
        import laspy
        las = laspy.read("fake")
        las.vlrs = []
        meta = gatekeeper._extract_crs(las, Path("test.las"))
        assert meta.is_valid is False
        assert any("No CRS" in e for e in meta.validation_errors)

    def test_epsg_not_in_allowed_list(self, gatekeeper):
        import laspy
        las = laspy.read("fake")

        fake_vlr = MagicMock()
        fake_vlr.record_id = 2112
        fake_vlr.record_data = b'GEOGCS["WGS 84"]'
        las.vlrs = [fake_vlr]

        with patch("totali.geodetic.gatekeeper.CRS") as mock_crs_cls:
            mock_crs = MagicMock()
            mock_crs.to_epsg.return_value = 9999
            mock_crs.datum.name = "WGS 84"
            mock_crs_cls.from_wkt.return_value = mock_crs
            meta = gatekeeper._extract_crs(las, Path("test.las"))

        assert meta.is_valid is False
        assert any("not in allowed" in e for e in meta.validation_errors)


class TestComputeStats:
    def test_stats_from_las(self, gatekeeper):
        import laspy
        las = laspy.read("fake")
        meta = CRSMetadata(epsg_code=2231, is_valid=True)
        stats = gatekeeper._compute_stats(las, Path("test.las"), meta)
        assert stats.point_count == len(las.points)
        assert stats.bounds_min is not None
        assert stats.bounds_max is not None
        assert stats.has_intensity is True


class TestFileHashing:
    def test_hash_matches_manual(self, gatekeeper, tmp_path):
        test_file = tmp_path / "data.bin"
        test_file.write_bytes(b"deterministic content for hashing")
        result = gatekeeper._hash_file(test_file)
        expected = hashlib.sha256(b"deterministic content for hashing").hexdigest()
        assert result == expected


class TestTransforms:
    def test_no_transform_when_crs_matches(self, gatekeeper):
        import laspy
        las = laspy.read("fake")
        crs = CRSMetadata(epsg_code=2231, is_valid=True)
        xyz, transformed = gatekeeper._apply_transforms(las, crs)
        assert transformed is False
        assert xyz.shape[1] == 3

    def test_transform_applied_when_crs_differs(self, gatekeeper):
        import laspy
        las = laspy.read("fake")
        crs = CRSMetadata(epsg_code=4326, is_valid=True)
        xyz, transformed = gatekeeper._apply_transforms(las, crs)
        assert transformed is True
        assert xyz.shape[1] == 3


class TestPhaseRun:
    def test_run_returns_expected_data_keys_on_success(self, audit_logger, tmp_path):
        fake_las = tmp_path / "input.las"
        fake_las.write_bytes(b"\x00" * 227)
        output_dir = tmp_path / "out"
        output_dir.mkdir()

        config = {
            "allowed_crs": ["EPSG:2231"],
            "reject_on_missing_crs": True,
            "geoid_model": "GEOID18",
            "elevation_unit": "US_survey_foot",
        }
        gatekeeper = GeodeticGatekeeper(config, audit_logger)

        ctx = PipelineContext(input_path=str(fake_las), output_dir=output_dir)
        fake_las_data = MagicMock()
        fake_las_data.vlrs = []
        fake_las_data.points = [1, 2, 3]
        fake_las_data.x = np.array([0.0, 1.0, 2.0])
        fake_las_data.y = np.array([0.0, 1.0, 2.0])
        fake_las_data.z = np.array([10.0, 11.0, 12.0])
        fake_las_data.intensity = np.array([100, 110, 120])
        fake_las_data.classification = np.array([2, 2, 6])

        fake_crs = MagicMock()
        fake_crs.to_epsg.return_value = 2231
        fake_crs.datum.name = "NAD83(2011)"
        fake_las_data.header.parse_crs.return_value = fake_crs

        with patch("totali.geodetic.gatekeeper.laspy.read", return_value=fake_las_data):
            result = gatekeeper.run(ctx)

        assert isinstance(result, PhaseResult)
        assert result.phase == "geodetic"
        assert result.success is True
        assert set(result.data.keys()) == {"points_xyz", "las", "crs", "stats", "input_hash"}
        crs = result.data["crs"]
        assert crs.epsg_code == 2231
        assert crs.is_valid is True
        assert crs.geoid_model == "GEOID18"
        assert crs.source_datum == "NAD83(2011)"
        assert crs.horizontal_unit == "US_survey_foot"
        assert crs.vertical_unit == "US_survey_foot"

    def test_run_rejects_missing_crs_with_invalid_epsg_zero(self, audit_logger, tmp_path):
        """Missing CRS must never fall through as EPSG:0."""
        fake_las = tmp_path / "input.las"
        fake_las.write_bytes(b"\x00" * 100)
        output_dir = tmp_path / "out"
        output_dir.mkdir()

        config = {
            "allowed_crs": ["EPSG:2231"],
            "reject_on_missing_crs": False,
            "geoid_model": "GEOID18",
            "elevation_unit": "US_survey_foot",
        }
        gk = GeodeticGatekeeper(config, audit_logger)
        ctx = PipelineContext(input_path=str(fake_las), output_dir=output_dir)
        result = gk.run(ctx)

        assert result.success is False
        assert "CRS validation failed" in result.message
        assert "EPSG:0" in result.message
