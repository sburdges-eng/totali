"""G-9: geodetic phase determinism — identical inputs produce identical artifacts."""

from pathlib import Path

import pytest

from totali.audit.logger import AuditLogger
from totali.geodetic.gatekeeper import GeodeticGatekeeper
from totali.pipeline.context import PipelineContext
from totali.pipeline.models import CRSMetadata

_G9_CFG = {
    "allowed_crs": ["EPSG:2231", "EPSG:2233"],
    "reject_on_missing_crs": True,
    "geoid_model": "GEOID18",
    "elevation_unit": "US_survey_foot",
    "crs_inference_enabled": True,
    "jurisdiction_zones": [
        {
            "epsg": 2231,
            "name": "NAD83 Colorado North (ftUS)",
            "xy_min": [-1.0, -1.0],
            "xy_max": [2000.0, 2000.0],
        },
    ],
}


@pytest.fixture
def gatekeeper(audit_logger):
    config = {
        "allowed_crs": ["EPSG:2231"],
        "reject_on_missing_crs": True,
        "geoid_model": "GEOID18",
        "elevation_unit": "US_survey_foot",
        "unit_tolerance_ft": 0.01,
    }
    return GeodeticGatekeeper(config, audit_logger), audit_logger


def _meta(h: str, v: str) -> CRSMetadata:
    return CRSMetadata(epsg_code=2231, horizontal_unit=h, vertical_unit=v)


class TestValidateUnitsDeterminism:
    def test_same_input_same_payload(self, gatekeeper):
        gk, audit = gatekeeper
        gk._validate_units(_meta("meter", "meter"), Path("same.las"))
        gk._validate_units(_meta("meter", "meter"), Path("same.las"))
        events = [e["data"] for e in audit.get_events("unit_rejected")]
        assert len(events) == 2
        # Remove any time-varying fields — payload content is deterministic.
        assert events[0] == events[1]

    def test_order_preserved(self, gatekeeper):
        gk, audit = gatekeeper
        for i in range(5):
            gk._validate_units(
                _meta("US_survey_foot", "US_survey_foot"),
                Path(f"f{i}.las"),
            )
        events = audit.get_events("unit_validated")
        files = [e["data"]["file"] for e in events]
        assert files == [f"f{i}.las" for i in range(5)]


class TestGeodeticReportDeterminism:
    """Two geodetic runs on the same LAS yield byte-identical reports (G-9)."""

    @staticmethod
    def _run_once(tmp_path: Path, las: Path, suffix: str) -> bytes:
        out = tmp_path / f"out_{suffix}"
        out.mkdir()
        audit = AuditLogger(
            log_dir=str(tmp_path / f"audit_{suffix}"),
            project_id=f"g9_{suffix}",
        )
        gk = GeodeticGatekeeper(_G9_CFG, audit)
        ctx = PipelineContext(input_path=str(las), output_dir=out)
        result = gk.run(ctx)
        assert result.success is True, result.message
        report = out / f"{las.stem}_geodetic_report.json"
        assert report.exists()
        return report.read_bytes()

    def test_geodetic_report_byte_identical_across_runs(self, tmp_path):
        las = tmp_path / "golden.las"
        las.write_bytes(b"\x00" * 256)

        first = self._run_once(tmp_path, las, "a")
        second = self._run_once(tmp_path, las, "b")
        assert first == second
