"""U5: pipeline timing instrumentation (pilot baseline for SC2 / R7).

Aggregates per-phase + total wall-clock timing into a metrics artifact written
alongside the audit record, with an optional operator-supplied manual baseline.

SC2 guard: NO time-savings percentage is computed or emitted anywhere. The
baseline is *measured* during the pilot; the public claim is set
post-measurement (KTD5). These tests pin that raw-seconds-only contract.
"""

from __future__ import annotations

import json

from totali.pipeline.metrics import PhaseTiming, RunMetrics
from totali.pipeline.models import PhaseResult, PipelineResult


def _sample_result():
    return PipelineResult(
        project_id="proj-1",
        duration_sec=1.5,
        phases=[
            PhaseResult(phase="geodetic", success=True, duration_sec=0.4),
            PhaseResult(phase="segment", success=True, duration_sec=0.6),
            PhaseResult(phase="extract", success=False, duration_sec=0.5),
        ],
    )


class TestFromPipelineResult:
    def test_captures_total_and_per_phase(self):
        metrics = RunMetrics.from_pipeline_result(_sample_result())
        assert metrics.project_id == "proj-1"
        assert metrics.total_duration_sec == 1.5
        assert len(metrics.phases) == 3
        assert all(isinstance(p, PhaseTiming) for p in metrics.phases)
        assert metrics.phases[0].phase == "geodetic"
        assert metrics.phases[0].duration_sec == 0.4
        assert metrics.phases[2].success is False


class TestManualBaseline:
    def test_absent_by_default(self):
        d = RunMetrics.from_pipeline_result(_sample_result()).to_dict()
        assert "manual_baseline_sec" not in d

    def test_present_when_provided(self):
        d = RunMetrics.from_pipeline_result(
            _sample_result(), manual_baseline_sec=3600.0
        ).to_dict()
        assert d["manual_baseline_sec"] == 3600.0


class TestNoPercentClaim:
    def test_output_contains_no_savings_percentage(self):
        # With a manual baseline present (the tempting moment to compute a %),
        # the serialized output must still carry NO percentage / savings claim.
        metrics = RunMetrics.from_pipeline_result(
            _sample_result(), manual_baseline_sec=3600.0
        )
        blob = json.dumps(metrics.to_dict()).lower()
        for forbidden in ("percent", "%", "saving", "reduction", "faster", "speedup"):
            assert forbidden not in blob, f"SC2 guard: {forbidden!r} must not appear"


class TestArtifactWrite:
    def test_writes_alongside_audit_record_and_emits_event(self, audit_logger):
        metrics = RunMetrics.from_pipeline_result(_sample_result())
        artifact = metrics.write(audit_logger)

        # Alongside the audit record (same directory as the audit log file).
        assert artifact.exists()
        assert artifact.parent == audit_logger.log_path.parent

        data = json.loads(artifact.read_text())
        assert data["total_duration_sec"] == 1.5
        assert [p["phase"] for p in data["phases"]] == ["geodetic", "segment", "extract"]

        events = audit_logger.get_events("metrics_recorded")
        assert len(events) == 1
        assert events[0]["data"]["total_duration_sec"] == 1.5


class TestOrchestratorIntegration:
    def test_run_records_metrics_artifact(self, audit_logger, sample_config, tmp_path):
        from totali.pipeline.orchestrator import PipelineOrchestrator

        out = tmp_path / "out"
        out.mkdir()
        las = tmp_path / "input.las"
        las.write_bytes(b"\x00" * 100)

        orch = PipelineOrchestrator(sample_config, audit_logger, out)
        orch.run(str(las), phase="geodetic")

        artifact = audit_logger.log_path.with_suffix(".metrics.json")
        assert artifact.exists(), "orchestrator run must emit a timing metrics artifact"
        data = json.loads(artifact.read_text())
        assert "total_duration_sec" in data
        assert len(data["phases"]) >= 1
        assert len(audit_logger.get_events("metrics_recorded")) == 1

    def test_run_records_manual_baseline_when_passed(self, audit_logger, sample_config, tmp_path):
        from totali.pipeline.orchestrator import PipelineOrchestrator

        out = tmp_path / "out"
        out.mkdir()
        las = tmp_path / "input.las"
        las.write_bytes(b"\x00" * 100)

        orch = PipelineOrchestrator(sample_config, audit_logger, out)
        orch.run(str(las), phase="geodetic", manual_baseline_sec=1800.0)

        data = json.loads(audit_logger.log_path.with_suffix(".metrics.json").read_text())
        assert data["manual_baseline_sec"] == 1800.0
