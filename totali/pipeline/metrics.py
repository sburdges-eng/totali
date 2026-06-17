"""U5: pipeline timing instrumentation — the pilot's measured baseline (R7/SC2).

Aggregates per-phase and total wall-clock timing from a :class:`PipelineResult`
into a metrics artifact written *alongside the audit record*, optionally carrying
an operator-supplied manual baseline for later comparison.

IMPORTANT (KTD5 / SC2): this module records **raw seconds only**. It deliberately
does NOT compute or emit any time-savings percentage. The manual baseline is
measured during the pilot and the public claim is set post-measurement — there
is no committed number to fabricate here.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional


@dataclass
class PhaseTiming:
    phase: str
    duration_sec: float
    success: bool


@dataclass
class RunMetrics:
    project_id: str
    total_duration_sec: float
    phases: list[PhaseTiming] = field(default_factory=list)
    # Operator-supplied wall-clock for the equivalent manual drafting effort.
    # Absent by default — never fabricated.
    manual_baseline_sec: Optional[float] = None

    @classmethod
    def from_pipeline_result(
        cls, result, manual_baseline_sec: Optional[float] = None
    ) -> "RunMetrics":
        return cls(
            project_id=result.project_id,
            total_duration_sec=result.duration_sec,
            phases=[
                PhaseTiming(p.phase, p.duration_sec, p.success) for p in result.phases
            ],
            manual_baseline_sec=manual_baseline_sec,
        )

    def to_dict(self) -> dict:
        out: dict = {
            "project_id": self.project_id,
            "total_duration_sec": self.total_duration_sec,
            "phases": [
                {"phase": p.phase, "duration_sec": p.duration_sec, "success": p.success}
                for p in self.phases
            ],
        }
        # Optional and absent by default. No derived percentage — see KTD5/SC2.
        if self.manual_baseline_sec is not None:
            out["manual_baseline_sec"] = self.manual_baseline_sec
        return out

    def write(self, audit, output_dir: Optional[Path] = None) -> Path:
        """Write the metrics artifact next to the audit record and emit a
        `metrics_recorded` audit event. Returns the artifact path.

        The artifact lives beside the audit log (``<audit-log>.metrics.json``) so
        the timing record travels with the chain-of-custody it describes.
        """
        log_path = Path(audit.log_path)
        artifact = log_path.with_suffix(".metrics.json")
        artifact.write_text(json.dumps(self.to_dict(), indent=2))

        audit.log("metrics_recorded", {
            "artifact": str(artifact),
            "total_duration_sec": self.total_duration_sec,
            "phase_count": len(self.phases),
            "manual_baseline_sec": self.manual_baseline_sec,
        })
        return artifact
