"""
Phase 6: Integration & End-to-End Tests
=========================================
Cross-component integration tests that exercise the full pipeline
(geodetic → segment → extract → shield → lint) as a connected whole.

Validates context propagation, audit chain integrity, error halt behavior,
selective execution, and output contract completeness.
"""

import json
import re
from pathlib import Path
from unittest.mock import patch, MagicMock

import numpy as np
import pytest

from totali.pipeline.orchestrator import PipelineOrchestrator, PHASE_ORDER
from totali.pipeline.models import (
    PipelineResult,
    PhaseResult,
    GeometryStatus,
    CRSMetadata,
    ClassificationResult,
    ExtractionResult,
    HealingReport,
)
from totali.pipeline.context import PipelineContext
from totali.audit.logger import AuditLogger


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _fake_las_data():
    """Build a mock LasData with valid CRS VLR for end-to-end."""
    mock_las = MagicMock()
    rng = np.random.default_rng(42)
    n = 200
    mock_las.x = rng.uniform(0, 1000, n)
    mock_las.y = rng.uniform(0, 1000, n)
    mock_las.z = np.concatenate([
        rng.uniform(100, 105, 80),
        rng.uniform(105, 115, 40),
        rng.uniform(115, 130, 40),
        rng.uniform(130, 160, 40),
    ])
    mock_las.points = list(range(n))
    mock_las.classification = rng.choice([0, 2, 3, 5, 6], n)
    mock_las.intensity = rng.integers(0, 65535, n, dtype=np.uint16)
    mock_las.return_number = np.ones(n, dtype=np.uint8)
    mock_las.number_of_returns = np.ones(n, dtype=np.uint8)
    mock_las.vlrs = []

    fake_crs = MagicMock()
    fake_crs.to_epsg.return_value = 2231
    fake_crs.datum.name = "NAD83(2011)"
    mock_las.header = MagicMock()
    mock_las.header.parse_crs.return_value = fake_crs
    mock_las.header.point_format = MagicMock(id=6)

    def write_noop(path):
        Path(path).touch()

    mock_las.write = write_noop
    return mock_las


def _make_orchestrator(tmp_path, audit, config_overrides=None):
    """Instantiate an orchestrator with a standard test config."""
    config = {
        "project": {"name": "integration_test", "version": "0.1.0"},
        "geodetic": {
            "allowed_crs": ["EPSG:2231"],
            "reject_on_missing_crs": True,
            "reject_on_mixed_datum": True,
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
            "classes": {0: "unclassified", 2: "ground", 3: "low_veg", 5: "high_veg", 6: "building"},
        },
        "extraction": {
            "dtm": {"max_triangle_edge_length": 500.0, "thin_factor": 1.0},
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
            "log_dir": str(tmp_path / "audit"),
            "log_format": "jsonl",
            "hash_algorithm": "sha256",
        },
    }
    if config_overrides:
        for k, v in config_overrides.items():
            if isinstance(v, dict) and k in config:
                config[k].update(v)
            else:
                config[k] = v

    output_dir = tmp_path / "output"
    output_dir.mkdir(parents=True, exist_ok=True)
    return PipelineOrchestrator(config, audit, output_dir)


@pytest.fixture
def audit(tmp_path):
    return AuditLogger(
        log_dir=str(tmp_path / "audit"),
        project_id="integration_test",
    )


@pytest.fixture
def fake_input(tmp_path):
    """Create a fake .las input file on disk."""
    f = tmp_path / "test_input.las"
    f.write_bytes(b"\x00" * 227)
    return str(f)


# ===================================================================
# End-to-End: Full Pipeline Run
# ===================================================================


class TestFullPipelineEndToEnd:
    """Runs all 5 phases in sequence with mocked I/O."""

    def test_all_five_phases_execute(self, tmp_path, audit, fake_input):
        orch = _make_orchestrator(tmp_path, audit)
        mock_las = _fake_las_data()

        with patch("totali.geodetic.gatekeeper.laspy.read", return_value=mock_las):
            result = orch.run(fake_input, phase="all")

        assert isinstance(result, PipelineResult)
        assert result.project_id == "integration_test"
        assert len(result.phases) == 5
        for pr in result.phases:
            assert pr.success is True, f"Phase {pr.phase} failed: {pr.message}"

    def test_phase_names_match_order(self, tmp_path, audit, fake_input):
        orch = _make_orchestrator(tmp_path, audit)
        mock_las = _fake_las_data()

        with patch("totali.geodetic.gatekeeper.laspy.read", return_value=mock_las):
            result = orch.run(fake_input)

        phase_names = [p.phase for p in result.phases]
        assert phase_names == ["geodetic", "segment", "extract", "shield", "lint"]

    def test_result_success_flag_true(self, tmp_path, audit, fake_input):
        orch = _make_orchestrator(tmp_path, audit)
        mock_las = _fake_las_data()

        with patch("totali.geodetic.gatekeeper.laspy.read", return_value=mock_las):
            result = orch.run(fake_input)

        assert result.success is True

    def test_duration_positive(self, tmp_path, audit, fake_input):
        orch = _make_orchestrator(tmp_path, audit)
        mock_las = _fake_las_data()

        with patch("totali.geodetic.gatekeeper.laspy.read", return_value=mock_las):
            result = orch.run(fake_input)

        assert result.duration_sec > 0
        for pr in result.phases:
            assert pr.duration_sec >= 0

    def test_output_files_produced(self, tmp_path, audit, fake_input):
        orch = _make_orchestrator(tmp_path, audit)
        mock_las = _fake_las_data()

        with patch("totali.geodetic.gatekeeper.laspy.read", return_value=mock_las):
            result = orch.run(fake_input)

        assert len(result.output_files) > 0
        non_las = [f for f in result.output_files if not str(f).endswith(".las")]
        assert len(non_las) > 0
        for f in non_las:
            assert Path(f).exists(), f"Output file missing: {f}"


# ===================================================================
# Context Propagation
# ===================================================================


class TestContextPropagation:
    """Verify that each phase passes required data downstream."""

    def test_geodetic_populates_crs(self, tmp_path, audit, fake_input):
        orch = _make_orchestrator(tmp_path, audit)
        mock_las = _fake_las_data()

        with patch("totali.geodetic.gatekeeper.laspy.read", return_value=mock_las):
            result = orch.run(fake_input)

        geo_data = result.phases[0].data
        assert "crs" in geo_data
        assert "points_xyz" in geo_data
        assert "input_hash" in geo_data
        assert geo_data["crs"].epsg_code == 2231

    def test_classification_flows_to_extraction(self, tmp_path, audit, fake_input):
        orch = _make_orchestrator(tmp_path, audit)
        mock_las = _fake_las_data()

        with patch("totali.geodetic.gatekeeper.laspy.read", return_value=mock_las):
            result = orch.run(fake_input)

        seg_data = result.phases[1].data
        assert "classification" in seg_data
        assert isinstance(seg_data["classification"], ClassificationResult)

    def test_extraction_flows_to_shielding(self, tmp_path, audit, fake_input):
        orch = _make_orchestrator(tmp_path, audit)
        mock_las = _fake_las_data()

        with patch("totali.geodetic.gatekeeper.laspy.read", return_value=mock_las):
            result = orch.run(fake_input)

        extract_data = result.phases[2].data
        assert "extraction" in extract_data
        assert isinstance(extract_data["extraction"], ExtractionResult)

    def test_manifest_flows_to_linting(self, tmp_path, audit, fake_input):
        orch = _make_orchestrator(tmp_path, audit)
        mock_las = _fake_las_data()

        with patch("totali.geodetic.gatekeeper.laspy.read", return_value=mock_las):
            result = orch.run(fake_input)

        shield_data = result.phases[3].data
        assert "manifest" in shield_data
        assert "entities" in shield_data["manifest"]

    def test_lint_items_all_start_as_draft(self, tmp_path, audit, fake_input):
        orch = _make_orchestrator(tmp_path, audit)
        mock_las = _fake_las_data()

        with patch("totali.geodetic.gatekeeper.laspy.read", return_value=mock_las):
            result = orch.run(fake_input)

        lint_data = result.phases[4].data
        for item in lint_data.get("lint_items", []):
            assert item.status == GeometryStatus.DRAFT

    def test_stats_populated_on_result(self, tmp_path, audit, fake_input):
        orch = _make_orchestrator(tmp_path, audit)
        mock_las = _fake_las_data()

        with patch("totali.geodetic.gatekeeper.laspy.read", return_value=mock_las):
            result = orch.run(fake_input)

        assert result.stats is not None
        assert result.stats.point_count > 0

    def test_classification_populated_on_result(self, tmp_path, audit, fake_input):
        orch = _make_orchestrator(tmp_path, audit)
        mock_las = _fake_las_data()

        with patch("totali.geodetic.gatekeeper.laspy.read", return_value=mock_las):
            result = orch.run(fake_input)

        assert result.classification is not None
        assert result.classification.labels is not None

    def test_extraction_populated_on_result(self, tmp_path, audit, fake_input):
        orch = _make_orchestrator(tmp_path, audit)
        mock_las = _fake_las_data()

        with patch("totali.geodetic.gatekeeper.laspy.read", return_value=mock_las):
            result = orch.run(fake_input)

        assert result.extraction is not None
        assert result.extraction.dtm_vertices is not None

    def test_healing_populated_on_result(self, tmp_path, audit, fake_input):
        orch = _make_orchestrator(tmp_path, audit)
        mock_las = _fake_las_data()

        with patch("totali.geodetic.gatekeeper.laspy.read", return_value=mock_las):
            result = orch.run(fake_input)

        assert result.healing is not None
        assert isinstance(result.healing, HealingReport)


# ===================================================================
# Audit Trail Integrity
# ===================================================================


class TestAuditTrailIntegrity:
    """Verify that a full pipeline run produces a valid audit chain."""

    def test_audit_chain_valid_after_full_run(self, tmp_path, audit, fake_input):
        orch = _make_orchestrator(tmp_path, audit)
        mock_las = _fake_las_data()

        with patch("totali.geodetic.gatekeeper.laspy.read", return_value=mock_las):
            orch.run(fake_input)

        valid, errors = audit.verify_chain()
        assert valid is True, f"Audit chain broken: {errors}"

    def test_audit_log_has_phase_events(self, tmp_path, audit, fake_input):
        orch = _make_orchestrator(tmp_path, audit)
        mock_las = _fake_las_data()

        with patch("totali.geodetic.gatekeeper.laspy.read", return_value=mock_las):
            orch.run(fake_input)

        events = audit.get_events()
        event_types = {e["event"] for e in events}

        assert "phase_start" in event_types
        assert "phase_complete" in event_types

    def test_ingest_event_logged(self, tmp_path, audit, fake_input):
        orch = _make_orchestrator(tmp_path, audit)
        mock_las = _fake_las_data()

        with patch("totali.geodetic.gatekeeper.laspy.read", return_value=mock_las):
            orch.run(fake_input)

        ingest_events = audit.get_events("ingest")
        assert len(ingest_events) >= 1
        assert "sha256" in ingest_events[0]["data"]

    def test_classify_event_logged(self, tmp_path, audit, fake_input):
        orch = _make_orchestrator(tmp_path, audit)
        mock_las = _fake_las_data()

        with patch("totali.geodetic.gatekeeper.laspy.read", return_value=mock_las):
            orch.run(fake_input)

        classify_events = audit.get_events("classify")
        assert len(classify_events) >= 1

    def test_extract_event_logged(self, tmp_path, audit, fake_input):
        orch = _make_orchestrator(tmp_path, audit)
        mock_las = _fake_las_data()

        with patch("totali.geodetic.gatekeeper.laspy.read", return_value=mock_las):
            orch.run(fake_input)

        extract_events = audit.get_events("extract")
        assert len(extract_events) >= 1

    def test_insert_events_logged(self, tmp_path, audit, fake_input):
        orch = _make_orchestrator(tmp_path, audit)
        mock_las = _fake_las_data()

        with patch("totali.geodetic.gatekeeper.laspy.read", return_value=mock_las):
            orch.run(fake_input)

        insert_events = audit.get_events("insert")
        assert len(insert_events) >= 1
        for ie in insert_events:
            assert ie["data"]["status"] == "DRAFT"

    def test_lint_complete_event_logged(self, tmp_path, audit, fake_input):
        orch = _make_orchestrator(tmp_path, audit)
        mock_las = _fake_las_data()

        with patch("totali.geodetic.gatekeeper.laspy.read", return_value=mock_las):
            orch.run(fake_input)

        lint_events = audit.get_events("lint_complete")
        assert len(lint_events) == 1
        assert lint_events[0]["data"]["auto_promote"] is False

    def test_audit_summary_covers_all_phases(self, tmp_path, audit, fake_input):
        orch = _make_orchestrator(tmp_path, audit)
        mock_las = _fake_las_data()

        with patch("totali.geodetic.gatekeeper.laspy.read", return_value=mock_las):
            orch.run(fake_input)

        summary = audit.summary()
        assert summary["total_events"] > 0
        assert summary["chain_valid"] is True

    def test_five_phase_start_events(self, tmp_path, audit, fake_input):
        orch = _make_orchestrator(tmp_path, audit)
        mock_las = _fake_las_data()

        with patch("totali.geodetic.gatekeeper.laspy.read", return_value=mock_las):
            orch.run(fake_input)

        starts = audit.get_events("phase_start")
        assert len(starts) == 5
        started_phases = [e["data"]["phase"] for e in starts]
        assert started_phases == PHASE_ORDER

    def test_five_phase_complete_events(self, tmp_path, audit, fake_input):
        orch = _make_orchestrator(tmp_path, audit)
        mock_las = _fake_las_data()

        with patch("totali.geodetic.gatekeeper.laspy.read", return_value=mock_las):
            orch.run(fake_input)

        completes = audit.get_events("phase_complete")
        assert len(completes) == 5
        completed_phases = [e["data"]["phase"] for e in completes]
        assert completed_phases == PHASE_ORDER


# ===================================================================
# Error Propagation & Halt
# ===================================================================


class TestErrorPropagation:
    """Pipeline must halt on first phase failure and report correctly."""

    def test_geodetic_failure_halts_pipeline(self, tmp_path, audit, fake_input):
        orch = _make_orchestrator(tmp_path, audit)
        mock_las = _fake_las_data()
        mock_las.vlrs = []
        mock_las.header.parse_crs.return_value = None

        with patch("totali.geodetic.gatekeeper.laspy.read", return_value=mock_las):
            result = orch.run(fake_input)

        assert result.success is False
        assert len(result.phases) == 1
        assert result.phases[0].phase == "geodetic"

    def test_geodetic_failure_logged(self, tmp_path, audit, fake_input):
        orch = _make_orchestrator(tmp_path, audit)
        mock_las = _fake_las_data()
        mock_las.vlrs = []
        mock_las.header.parse_crs.return_value = None

        with patch("totali.geodetic.gatekeeper.laspy.read", return_value=mock_las):
            orch.run(fake_input)

        failed = audit.get_events("phase_failed")
        assert len(failed) >= 1
        assert failed[0]["data"]["phase"] == "geodetic"

    def test_segment_receives_no_data_on_geodetic_fail(self, tmp_path, audit, fake_input):
        """Segment phase should never execute if geodetic fails."""
        orch = _make_orchestrator(tmp_path, audit)
        mock_las = _fake_las_data()
        mock_las.vlrs = []
        mock_las.header.parse_crs.return_value = None

        with patch("totali.geodetic.gatekeeper.laspy.read", return_value=mock_las):
            result = orch.run(fake_input)

        phase_names = [p.phase for p in result.phases]
        assert "segment" not in phase_names

    def test_no_auto_promote_even_on_success(self, tmp_path, audit, fake_input):
        """Even a fully successful run must NOT auto-promote any items."""
        orch = _make_orchestrator(
            tmp_path, audit,
            config_overrides={"linting": {"auto_promote": True, "require_pls_signature": True}},
        )
        mock_las = _fake_las_data()

        with patch("totali.geodetic.gatekeeper.laspy.read", return_value=mock_las):
            result = orch.run(fake_input)

        lint_data = result.phases[-1].data
        for item in lint_data.get("lint_items", []):
            assert item.status == GeometryStatus.DRAFT, \
                f"Item {item.item_id} was promoted to {item.status} — auto_promote must be hardcoded False"


# ===================================================================
# Selective Phase Execution
# ===================================================================


class TestSelectiveExecution:
    """Test running a single phase via the --phase CLI flag."""

    def test_run_geodetic_only(self, tmp_path, audit, fake_input):
        orch = _make_orchestrator(tmp_path, audit)
        mock_las = _fake_las_data()

        with patch("totali.geodetic.gatekeeper.laspy.read", return_value=mock_las):
            result = orch.run(fake_input, phase="geodetic")

        assert len(result.phases) == 1
        assert result.phases[0].phase == "geodetic"
        assert result.phases[0].success is True

    def test_run_segment_only_runs_prerequisites_then_segment(self, tmp_path, audit, fake_input):
        """Running phase=segment runs geodetic first so segment receives populated context."""
        orch = _make_orchestrator(tmp_path, audit)
        mock_las = _fake_las_data()

        with patch("totali.geodetic.gatekeeper.laspy.read", return_value=mock_las):
            result = orch.run(fake_input, phase="segment")

        assert result.success is True
        assert len(result.phases) == 2
        assert result.phases[0].phase == "geodetic"
        assert result.phases[1].phase == "segment"
        assert result.phases[1].data is not None
        assert "classification" in result.phases[1].data


# ===================================================================
# Output File Contracts
# ===================================================================


class TestOutputContracts:
    """Verify that all expected output files and formats are produced."""

    def test_geodetic_report_is_valid_json(self, tmp_path, audit, fake_input):
        orch = _make_orchestrator(tmp_path, audit)
        mock_las = _fake_las_data()

        with patch("totali.geodetic.gatekeeper.laspy.read", return_value=mock_las):
            result = orch.run(fake_input)

        geo_outputs = result.phases[0].output_files
        json_files = [f for f in geo_outputs if str(f).endswith(".json")]
        assert len(json_files) >= 1
        with open(json_files[0], "r") as f:
            report = json.load(f)
        assert "crs" in report
        assert "validation_passed" in report
        assert report["validation_passed"] is True

    def test_extraction_report_is_valid_json(self, tmp_path, audit, fake_input):
        orch = _make_orchestrator(tmp_path, audit)
        mock_las = _fake_las_data()

        with patch("totali.geodetic.gatekeeper.laspy.read", return_value=mock_las):
            result = orch.run(fake_input)

        extract_outputs = result.phases[2].output_files
        json_files = [f for f in extract_outputs if str(f).endswith(".json")]
        assert len(json_files) >= 1
        with open(json_files[0], "r") as f:
            report = json.load(f)
        assert "dtm_vertices" in report
        assert "breaklines" in report

    def test_entity_manifest_is_valid_json(self, tmp_path, audit, fake_input):
        orch = _make_orchestrator(tmp_path, audit)
        mock_las = _fake_las_data()

        with patch("totali.geodetic.gatekeeper.laspy.read", return_value=mock_las):
            result = orch.run(fake_input)

        shield_outputs = result.phases[3].output_files
        manifest_files = [f for f in shield_outputs if str(f).endswith("entity_manifest.json")]
        assert len(manifest_files) >= 1
        with open(manifest_files[0], "r") as f:
            manifest = json.load(f)
        assert "entities" in manifest
        for entity in manifest["entities"]:
            assert "id" in entity
            assert "type" in entity
            assert "layer" in entity
            assert entity["status"] == "DRAFT"

    def test_dxf_output_exists(self, tmp_path, audit, fake_input):
        orch = _make_orchestrator(tmp_path, audit)
        mock_las = _fake_las_data()

        with patch("totali.geodetic.gatekeeper.laspy.read", return_value=mock_las):
            result = orch.run(fake_input)

        shield_outputs = result.phases[3].output_files
        dxf_files = [f for f in shield_outputs if str(f).endswith(".dxf")]
        assert len(dxf_files) >= 1
        assert Path(dxf_files[0]).exists()

    def test_lint_report_is_valid_json(self, tmp_path, audit, fake_input):
        orch = _make_orchestrator(tmp_path, audit)
        mock_las = _fake_las_data()

        with patch("totali.geodetic.gatekeeper.laspy.read", return_value=mock_las):
            result = orch.run(fake_input)

        lint_outputs = result.phases[4].output_files
        json_files = [f for f in lint_outputs if str(f).endswith(".json")]
        assert len(json_files) >= 1
        with open(json_files[0], "r") as f:
            report = json.load(f)
        assert "summary" in report
        assert "items" in report
        assert "certification_requirements" in report
        assert report["auto_promote"] is False

    def test_review_worksheet_exists(self, tmp_path, audit, fake_input):
        orch = _make_orchestrator(tmp_path, audit)
        mock_las = _fake_las_data()

        with patch("totali.geodetic.gatekeeper.laspy.read", return_value=mock_las):
            result = orch.run(fake_input)

        lint_outputs = result.phases[4].output_files
        txt_files = [f for f in lint_outputs if str(f).endswith(".txt")]
        assert len(txt_files) >= 1
        content = Path(txt_files[0]).read_text()
        assert "SURVEYOR REVIEW WORKSHEET" in content
        assert "PLS Signature" in content


# ===================================================================
# Reproducibility
# ===================================================================


class TestReproducibility:
    """Same input + config → same deterministic outputs (modulo timestamps)."""

    def test_two_runs_same_phase_count(self, tmp_path, audit, fake_input):
        mock_las = _fake_las_data()

        audit1 = AuditLogger(log_dir=str(tmp_path / "a1"), project_id="run1")
        orch1 = _make_orchestrator(tmp_path / "r1", audit1)

        audit2 = AuditLogger(log_dir=str(tmp_path / "a2"), project_id="run2")
        orch2 = _make_orchestrator(tmp_path / "r2", audit2)

        with patch("totali.geodetic.gatekeeper.laspy.read", return_value=mock_las):
            r1 = orch1.run(fake_input)
            r2 = orch2.run(fake_input)

        assert len(r1.phases) == len(r2.phases)
        for p1, p2 in zip(r1.phases, r2.phases):
            assert p1.phase == p2.phase
            assert p1.success == p2.success

    def test_two_runs_same_point_count(self, tmp_path, audit, fake_input):
        mock_las = _fake_las_data()

        audit1 = AuditLogger(log_dir=str(tmp_path / "b1"), project_id="run1")
        orch1 = _make_orchestrator(tmp_path / "s1", audit1)

        audit2 = AuditLogger(log_dir=str(tmp_path / "b2"), project_id="run2")
        orch2 = _make_orchestrator(tmp_path / "s2", audit2)

        with patch("totali.geodetic.gatekeeper.laspy.read", return_value=mock_las):
            r1 = orch1.run(fake_input)
            r2 = orch2.run(fake_input)

        assert r1.stats.point_count == r2.stats.point_count


# ===================================================================
# Certification Workflow Integration
# ===================================================================


class TestCertificationWorkflow:
    """Test the accept/reject/promote workflow integrated with audit."""

    def test_full_certification_lifecycle(self, tmp_path, audit, fake_input):
        """Simulate: run pipeline → accept all → promote → verify audit."""
        from totali.linting.surveyor_lint import SurveyorLinter

        orch = _make_orchestrator(tmp_path, audit)
        mock_las = _fake_las_data()

        with patch("totali.geodetic.gatekeeper.laspy.read", return_value=mock_las):
            result = orch.run(fake_input)

        lint_data = result.phases[-1].data
        items = lint_data.get("lint_items", [])
        if not items:
            pytest.skip("No lint items produced")

        for item in items:
            SurveyorLinter.accept_item(item, "PLS John Doe", audit, notes="Verified")

        promoted = SurveyorLinter.promote_to_certified(
            items, "John Doe", "PLS-12345", audit
        )
        assert promoted is True

        for item in items:
            assert item.status == GeometryStatus.CERTIFIED
            assert not item.layer.endswith("-DRAFT")

        valid, errors = audit.verify_chain()
        assert valid is True, f"Audit chain broken after certification: {errors}"

        certify_events = audit.get_events("certify")
        assert len(certify_events) == 1
        assert certify_events[0]["data"]["pls_license"] == "PLS-12345"


# ===================================================================
# CRS Quarantine Round-Trip
# ===================================================================


class TestCRSQuarantineRoundTrip:
    """Missing-CRS LAS → inference → quarantine → HITL resolve → resolution JSON."""

    def _las_no_crs_colorado_coords(self, n=100):
        """Mock LAS with no CRS metadata and Colorado SPCS-like coordinates."""
        mock_las = MagicMock()
        mock_las.vlrs = []
        mock_las.header = MagicMock()
        mock_las.header.parse_crs.return_value = None
        mock_las.header.point_format = MagicMock(id=6)
        mock_las.x = np.linspace(200_000, 300_000, n)
        mock_las.y = np.linspace(50_000, 150_000, n)
        mock_las.z = np.linspace(100, 200, n)
        mock_las.points = np.arange(n)
        mock_las.classification = np.full(n, 2)
        mock_las.intensity = np.zeros(n, dtype=np.uint16)
        mock_las.return_number = np.ones(n, dtype=np.uint8)
        mock_las.number_of_returns = np.ones(n, dtype=np.uint8)
        mock_las.write = lambda p: Path(p).parent.mkdir(parents=True, exist_ok=True)
        return mock_las

    def test_missing_crs_queues_for_human_review_and_resolve_writes_json(
        self, tmp_path, audit
    ):
        """Gatekeeper with inference on; ambiguous CRS → quarantine → resolve → JSON."""
        pytest.importorskip("flask")

        from totali.quarantine_ui.app import QUARANTINE_QUEUE, app, add_to_quarantine

        # Ensure quarantine UI is importable (add_to_quarantine used by gatekeeper)
        assert add_to_quarantine is not None

        QUARANTINE_QUEUE.clear()

        input_las = tmp_path / "no_crs_input.las"
        input_las.write_bytes(b"\x00" * 227)
        output_dir = tmp_path / "out"
        output_dir.mkdir()

        geodetic_config = {
            "allowed_crs": ["EPSG:2231", "EPSG:2232", "EPSG:2233"],
            "reject_on_missing_crs": False,
            "reject_on_mixed_datum": True,
            "geoid_model": "GEOID18",
            "elevation_unit": "US_survey_foot",
            "crs_inference": {
                "enabled": True,
                "confidence_threshold": 0.8,
                "auto_assign_high_confidence": True,
            },
        }
        from totali.geodetic.gatekeeper import GeodeticGatekeeper

        gatekeeper = GeodeticGatekeeper(geodetic_config, audit)
        ctx = PipelineContext(
            input_path=str(input_las),
            output_dir=output_dir,
        )
        mock_las = self._las_no_crs_colorado_coords()

        with patch("totali.geodetic.gatekeeper.laspy.read", return_value=mock_las):
            result = gatekeeper.run(ctx)

        assert result.success is False, "Expected CRS validation failure (quarantined)"
        assert "queued for human review" in result.message
        assert "CRS validation failed" in result.message

        # Extract item_id from message (e.g. "ID: abc12345")
        match = re.search(r"ID:\s*([a-f0-9]{8})", result.message)
        assert match is not None, f"Expected quarantine ID in message: {result.message}"
        item_id = match.group(1)
        assert item_id in QUARANTINE_QUEUE

        # Simulate HITL confirm via Flask API
        client = app.test_client()
        resp = client.post(
            "/api/resolve",
            json={"item_id": item_id, "epsg": 2231, "action": "confirm"},
            content_type="application/json",
        )
        assert resp.status_code == 200
        data = resp.get_json()
        assert data.get("success") is True
        assert data.get("epsg") == 2231

        # Resolution JSON written to input file's parent (path.parent)
        resolution_path = tmp_path / f"{item_id}_crs_resolution.json"
        assert resolution_path.exists(), f"Resolution file missing: {resolution_path}"

        with open(resolution_path) as f:
            resolution = json.load(f)
        assert resolution["item_id"] == item_id
        assert resolution["resolved_epsg"] == 2231
        assert resolution["action"] == "confirmed"
        assert resolution["source"] == "human_review"

        QUARANTINE_QUEUE.clear()
