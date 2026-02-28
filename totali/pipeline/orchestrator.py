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
        # Local imports to prevent circular cycles with phase-specific models
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
        result = PipelineResult(
            project_id=self.config.project.name
        )

        phases_to_run = PHASE_ORDER if phase == "all" else [phase]
        context = PipelineContext(
            input_path=input_path,
            output_dir=self.output_dir,
        )

        for phase_id in phases_to_run:
            if phase_id not in self.phases:
                continue

            processor = self.phases[phase_id]

            # Log phase start
            self.audit.log("phase_start", {"phase": phase_id, "project_id": result.project_id})

            try:
                phase_result: PhaseResult = processor.run(context)
                result.phases.append(phase_result)

                if not phase_result.success:
                    result.success = False
                    result.message = f"Phase '{phase_id}' failed: {phase_result.message}"
                    # Log phase failure
                    self.audit.log("phase_failure", {"phase": phase_id, "error": phase_result.message})
                    break

                # Merge phase output into context for downstream consumption
                if phase_result.data:
                    context.merge_data(phase_result.data)

                # Log phase completion
                self.audit.log("phase_complete", {"phase": phase_id})

            except Exception as e:
                # Log phase exception
                self.audit.log("phase_exception", {"phase": phase_id, "error": str(e)})
                result.success = False
                result.message = f"Phase '{phase_id}' crashed: {str(e)}"
                context.phase_status[phase_id] = "crashed"
                raise

        result.duration_sec = time.time() - t0
        return result
