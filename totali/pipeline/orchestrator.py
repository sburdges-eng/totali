"""
Pipeline Orchestrator
=====================
Runs phases in sequence, enforcing the division of labor:
  AI Classifies → Algorithms Measure → Humans Certify
"""

import time
from pathlib import Path

from totali.pipeline.models import PipelineResult, PhaseResult
from totali.pipeline.context import PipelineConfig, PipelineContext
from totali.audit.logger import AuditLogger


PHASE_ORDER = ["geodetic", "segment", "extract", "shield", "lint"]


class PipelineOrchestrator:
    def __init__(self, config: dict, audit: AuditLogger, output_dir: Path):
        self.config = PipelineConfig.model_validate(config)
        self.audit = audit
        self.output_dir = output_dir

        # Initialize phase processors
        from totali.geodetic.gatekeeper import GeodeticGatekeeper
        from totali.segmentation.classifier import PointCloudClassifier
        from totali.extraction.extractor import DeterministicExtractor
        from totali.cad_shielding.shield import CADShield
        from totali.linting.surveyor_lint import SurveyorLinter

        self.phases = {
            "geodetic": GeodeticGatekeeper(self.config.geodetic, audit),
            "segment": PointCloudClassifier(self.config.segmentation, audit),
            "extract": DeterministicExtractor(self.config.extraction, audit),
            "shield": CADShield(self.config.cad_shielding, audit),
            "lint": SurveyorLinter(self.config.linting, audit),
        }

    def run(self, input_path: str, phase: str = "all") -> PipelineResult:
        t0 = time.time()
        ctx = PipelineContext(
            input_path=input_path,
            output_dir=self.output_dir,
            input_hash="hash_placeholder"  # compute if needed
        )

        results = []
        phases_to_run = PHASE_ORDER if phase == "all" else [phase]

        for p_name in phases_to_run:
            if p_name not in self.phases:
                continue

            # Log phase start
            self.audit.log("phase_start", {"phase": p_name})

            # Run phase
            res = self.phases[p_name].run(ctx)
            results.append(res)

            # Log phase completion
            self.audit.log("phase_complete", {
                "phase": p_name,
                "success": res.success,
                "duration": res.duration_sec
            })

            if not res.success:
                break

            # Merge context
            if "extraction" in res.data:
                ctx.merge_data(res.data["extraction"])
            if "classification" in res.data:
                ctx.classification = res.data["classification"]
            if "points_xyz" in res.data:
                ctx.points_xyz = res.data["points_xyz"]
            if "crs" in res.data:
                ctx.crs = res.data["crs"]
            if "stats" in res.data:
                ctx.stats = res.data["stats"]

        duration = time.time() - t0
        success = all(r.success for r in results)

        return PipelineResult(
            project_id=self.config.project.name,
            success=success,
            phases=results,
            duration_sec=duration
        )
