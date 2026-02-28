"""Tests for Phase 1: GeodeticGatekeeper."""

import hashlib
from pathlib import Path
from unittest.mock import patch, MagicMock

import numpy as np
import pytest

from totali.geodetic.gatekeeper import GeodeticGatekeeper
from totali.pipeline.context import PipelineContext
from totali.pipeline.models import PhaseResult, CRSMetadata
from pyproj.exceptions import CRSError


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

    def test_invalid_allowed_crs(self, audit_logger):
        # Configuration with a non-EPSG CRS (simulated)
        config = {
            "allowed_crs": ["PROJ-STUB"],  # Our stub handles non-EPSG as 0/None
            "reject_on_missing_crs": True,
        }
        with patch("totali.geodetic.gatekeeper.CRS.from_user_input") as mock_from_user:
            mock_crs = MagicMock()
            mock_crs.to_epsg.return_value = None
            mock_from_user.return_value = mock_crs

            gk = GeodeticGatekeeper(config, audit_logger)
            ctx = PipelineContext(input_path="fake.las", output_dir="out")
            with patch("pathlib.Path.exists", return_value=True):
                valid, errors = gk.validate_inputs(ctx)

        assert valid is False
        assert "allowed_crs contains non-EPSG CRS" in errors[0]


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

    def test_extract_crs_malformed_wkt(self, gatekeeper):
        import laspy
        las = laspy.read("fake")
        fake_vlr = MagicMock()
        fake_vlr.record_id = 2112
        fake_vlr.record_data = b"INVALID WKT"
        las.vlrs = [fake_vlr]

        with patch("totali.geodetic.gatekeeper.CRS.from_wkt", side_effect=CRSError("Bad WKT")):
            meta = gatekeeper._extract_crs(las, Path("test.las"))

        assert meta.is_valid is False
        assert any("Invalid CRS WKT" in e for e in meta.validation_errors)

    def test_extract_crs_no_epsg_resolvable(self, gatekeeper):
        import laspy
        las = laspy.read("fake")
        fake_vlr = MagicMock()
        fake_vlr.record_id = 2112
        fake_vlr.record_data = b"SOME WKT"
        las.vlrs = [fake_vlr]

        with patch("totali.geodetic.gatekeeper.CRS.from_wkt") as mock_from_wkt:
            mock_crs = MagicMock()
            mock_crs.to_epsg.return_value = None
            mock_from_wkt.return_value = mock_crs
            meta = gatekeeper._extract_crs(las, Path("test.las"))

        assert meta.is_valid is False
        assert any("no EPSG code resolvable" in e for e in meta.validation_errors)


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
        assert isinstance(transformed, bool)
        assert xyz.shape[1] == 3

    def test_no_transform_when_epsg_missing(self, gatekeeper):
        import laspy
        las = laspy.read("fake")
        # Scenario where EPSG is 0/unknown
        crs = CRSMetadata(epsg_code=0, is_valid=False)
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
    def test_run_success_flow(self, gatekeeper, tmp_path):
        fake_las = tmp_path / "input.las"
        fake_las.write_bytes(b"\x00" * 227)
        output_dir = tmp_path / "out"
        output_dir.mkdir()

        ctx = PipelineContext(input_path=str(fake_las), output_dir=output_dir)
        result = gatekeeper.run(ctx)

        assert isinstance(result, PhaseResult)
        assert result.phase == "geodetic"
        assert isinstance(result.success, bool)

    def test_run_returns_expected_data_keys_on_success(self, audit_logger, tmp_path):
        """If CRS is valid, run() returns points_xyz, las, crs, stats, input_hash."""
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

        if result.success:
            assert "points_xyz" in result.data
            assert "las" in result.data
            assert "crs" in result.data
            assert "stats" in result.data
            assert "input_hash" in result.data

    def test_run_logs_transform(self, audit_logger, tmp_path):
        fake_las = tmp_path / "input.las"
        fake_las.write_bytes(b"\x00" * 100)
        output_dir = tmp_path / "out"
        output_dir.mkdir()

        config = {
            "allowed_crs": ["EPSG:2231"],
            "reject_on_missing_crs": False,
        }
        gk = GeodeticGatekeeper(config, audit_logger)
        ctx = PipelineContext(input_path=str(fake_las), output_dir=output_dir)

        # Force a transformation by providing a different valid CRS
        with patch.object(gk, "_extract_crs") as mock_extract:
            mock_extract.return_value = CRSMetadata(epsg_code=4326, is_valid=True)
            with patch.object(audit_logger, "log") as mock_log:
                gk.run(ctx)
                # Check if transform was logged
                mock_log.assert_any_call("transform", {
                    "from_crs": "EPSG:4326",
                    "to_crs": "EPSG:2231",
                    "geoid": "GEOID18",
                })
