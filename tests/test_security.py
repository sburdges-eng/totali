import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock
from totali.geodetic.gatekeeper import GeodeticGatekeeper
from totali.pipeline.context import PipelineContext

@pytest.fixture
def gatekeeper(audit_logger):
    config = {
        "allowed_crs": ["EPSG:2231"],
        "max_file_size_mb": 1,
        "max_point_count": 1000,
        "elevation_unit": "US_survey_foot",
    }
    return GeodeticGatekeeper(config, audit_logger)

class TestGeodeticSecurity:
    def test_file_size_limit_validation(self, gatekeeper, tmp_path, tmp_output):
        # Create a "large" file (2MB > 1MB limit)
        large_file = tmp_path / "large.las"
        with open(large_file, "wb") as f:
            f.seek(2 * 1024 * 1024 - 1)
            f.write(b"\0")

        ctx = PipelineContext(input_path=str(large_file), output_dir=tmp_output)
        valid, errors = gatekeeper.validate_inputs(ctx)

        assert not valid
        assert any("exceeds limit" in e for e in errors)

    def test_point_count_limit_run(self, gatekeeper, tmp_path, tmp_output):
        fake_las = tmp_path / "test.las"
        fake_las.write_bytes(b"\0" * 100)

        ctx = PipelineContext(input_path=str(fake_las), output_dir=tmp_output)

        # Mock laspy.open to return a header with high point count
        with patch("laspy.open") as mock_open:
            mock_reader = MagicMock()
            mock_reader.header.point_count = 5000 # > 1000 limit
            mock_open.return_value.__enter__.return_value = mock_reader

            result = gatekeeper.run(ctx)

            assert not result.success
            assert "Point count" in result.message
            mock_reader.read.assert_not_called()

    def test_safe_ingestion_flow(self, gatekeeper, tmp_path, tmp_output):
        small_file = tmp_path / "small.las"
        small_file.write_bytes(b"\0" * 100)

        ctx = PipelineContext(input_path=str(small_file), output_dir=tmp_output)

        with patch("laspy.open") as mock_open:
            mock_reader = MagicMock()
            mock_reader.header.point_count = 100 # < 1000 limit
            mock_reader.header.vlrs = []
            mock_reader.read.return_value = MagicMock()
            mock_open.return_value.__enter__.return_value = mock_reader

            # Should fail later due to CRS but should PASS point count check
            result = gatekeeper.run(ctx)

            # It will fail on CRS extraction because we have no VLRs,
            # but we want to see it passed the point count check.
            assert "Point count" not in result.message
            mock_reader.read.assert_called_once()
