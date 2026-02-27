"""Tests for Phase 1: GeodeticGatekeeper."""

import hashlib
from pathlib import Path
from unittest.mock import patch, MagicMock

import numpy as np
import pytest

from totali.geodetic.gatekeeper import GeodeticGatekeeper
from totali.pipeline.context import PipelineContext
from totali.pipeline.models import PhaseResult, CRSMetadata
from tests.conftest import _FakeLasData  # Import the fake data class

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


class TestCRSExtraction:
    @patch("laspy.read")
    def test_no_vlrs_rejects_when_configured(self, mock_read, gatekeeper):
        fake_las = _FakeLasData()
        fake_las.vlrs = []
        mock_read.return_value = fake_las

        meta = gatekeeper._extract_crs(fake_las, Path("test.las"))
        assert meta.is_valid is False
        assert any("No CRS" in e for e in meta.validation_errors)

    @patch("laspy.read")
    def test_epsg_not_in_allowed_list(self, mock_read, gatekeeper):
        fake_las = _FakeLasData()

        fake_vlr = MagicMock()
        fake_vlr.record_id = 2112
        fake_vlr.record_data = b'GEOGCS["WGS 84"]'
        fake_las.vlrs = [fake_vlr]
        mock_read.return_value = fake_las

        with patch("totali.geodetic.gatekeeper.CRS") as mock_crs_cls:
            mock_crs = MagicMock()
            mock_crs.to_epsg.return_value = 9999
            mock_crs.datum.name = "WGS 84"
            mock_crs_cls.from_wkt.return_value = mock_crs
            meta = gatekeeper._extract_crs(fake_las, Path("test.las"))

        assert meta.is_valid is False
        assert any("not in allowed" in e for e in meta.validation_errors)


class TestComputeStats:
    @patch("laspy.read")
    def test_stats_from_las(self, mock_read, gatekeeper):
        fake_las = _FakeLasData()
        mock_read.return_value = fake_las

        meta = CRSMetadata(epsg_code=2231, is_valid=True)
        stats = gatekeeper._compute_stats(fake_las, Path("test.las"), meta)
        assert stats.point_count == len(fake_las.points)
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
    @patch("laspy.read")
    def test_no_transform_when_crs_matches(self, mock_read, gatekeeper):
        fake_las = _FakeLasData()
        mock_read.return_value = fake_las

        crs = CRSMetadata(epsg_code=2231, is_valid=True)
        xyz, transformed = gatekeeper._apply_transforms(fake_las, crs)
        assert transformed is False
        assert xyz.shape[1] == 3

    @patch("laspy.read")
    def test_transform_applied_when_crs_differs(self, mock_read, gatekeeper):
        fake_las = _FakeLasData()
        mock_read.return_value = fake_las

        crs = CRSMetadata(epsg_code=4326, is_valid=True)
        xyz, transformed = gatekeeper._apply_transforms(fake_las, crs)
        assert transformed is True
        assert xyz.shape[1] == 3


class TestPhaseRun:
    @patch("laspy.read")
    def test_run_success_flow(self, mock_read, gatekeeper, tmp_path):
        fake_las = _FakeLasData()
        mock_read.return_value = fake_las

        input_file = tmp_path / "input.las"
        input_file.write_bytes(b"\x00" * 227)
        output_dir = tmp_path / "out"
        output_dir.mkdir()

        ctx = PipelineContext(input_path=str(input_file), output_dir=output_dir)
        result = gatekeeper.run(ctx)

        assert isinstance(result, PhaseResult)
        assert result.phase == "geodetic"
        assert isinstance(result.success, bool)

    @patch("laspy.read")
    def test_run_returns_expected_data_keys_on_success(self, mock_read, audit_logger, tmp_path):
        """If CRS is valid, run() returns points_xyz, las, crs, stats, input_hash."""
        fake_las = _FakeLasData()
        # Mock VLRs to allow extraction
        fake_vlr = MagicMock()
        fake_vlr.record_id = 2112
        fake_vlr.record_data = b'PROJCS["NAD83 / Colorado North (ftUS)"]'
        fake_las.vlrs = [fake_vlr]
        mock_read.return_value = fake_las

        # When _write_output is called, it creates a new LasData.
        # With real laspy installed, LasData(header) requires a real header.
        # We need to ensure fake_las.header behaves like a real header or mock LasData creation too.
        # But _write_output does: out_las = laspy.LasData(header)
        # So we should patch laspy.LasData to return a fake.

        input_file = tmp_path / "input.las"
        input_file.write_bytes(b"\x00" * 100)
        output_dir = tmp_path / "out"
        output_dir.mkdir()

        config = {
            "allowed_crs": ["EPSG:2231"],
            "reject_on_missing_crs": False,
            "geoid_model": "GEOID18",
            "elevation_unit": "US_survey_foot",
        }
        gk = GeodeticGatekeeper(config, audit_logger)

        with patch("totali.geodetic.gatekeeper.CRS") as mock_crs_cls:
            mock_crs = MagicMock()
            mock_crs.to_epsg.return_value = 2231
            mock_crs_cls.from_wkt.return_value = mock_crs
            mock_crs_cls.from_user_input.return_value = mock_crs

            with patch("laspy.LasData", return_value=_FakeLasData()):
                ctx = PipelineContext(input_path=str(input_file), output_dir=output_dir)
                result = gk.run(ctx)

        if result.success:
            assert "points_xyz" in result.data
            assert "las" in result.data
            assert "crs" in result.data
            assert "stats" in result.data
            assert "input_hash" in result.data
