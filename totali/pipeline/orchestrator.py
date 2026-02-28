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

        # Deferred imports to avoid circular dependencies
        from totali.geodetic.gatekeeper import GeodeticGatekeeper
        from totali.segmentation.classifier import PointCloudClassifier
        from totali.extraction.extractor import DeterministicExtractor
        from totali.cad_shielding.shield import CADShield
        from totali.linting.surveyor_lint import SurveyorLinter

        # Initialize phase processors
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

        for phase_name in phases_to_run:
            processor = self.phases[phase_name]
            self.audit.log(f"phase_start", {"phase": phase_name})

            pt0 = time.time()
            try:
                valid, errors = processor.validate_inputs(context)
                if not valid:
                    phase_result = PhaseResult(
                        phase=phase_name,
                        success=False,
                        duration_sec=time.time() - pt0,
                        message=f"Input validation failed: {errors}",
                    )
                    result.phases.append(phase_result)
                    result.success = False
                    context.phase_status[phase_name] = "failed_validation"
                    context.errors.extend(errors)
                    self.audit.log("phase_failed", {
                        "phase": phase_name,
                        "message": phase_result.message,
                    })
                    break

                phase_result = processor.run(context)
                phase_result.duration_sec = time.time() - pt0

                if not phase_result.success:
                    self.audit.log("phase_failed", {
                        "phase": phase_name,
                        "message": phase_result.message,
                    })
                    result.success = False
                    result.phases.append(phase_result)
                    context.phase_status[phase_name] = "failed"
                    context.errors.append(phase_result.message)
                    break

                # Pass outputs forward as context for next phase
                context.merge_data(phase_result.data)
                context.last_output_files = phase_result.output_files
                context.phase_status[phase_name] = "success"
                result.phases.append(phase_result)
                result.output_files.extend(phase_result.output_files)

                self.audit.log("phase_complete", {
                    "phase": phase_name,
                    "duration_sec": phase_result.duration_sec,
                    "outputs": [str(f) for f in phase_result.output_files],
                })

            except Exception as e:
                phase_result = PhaseResult(
                    phase=phase_name, success=False,
                    duration_sec=time.time() - pt0,
                    message=f"Exception: {e}",
                )
                result.phases.append(phase_result)
                result.success = False
                context.phase_status[phase_name] = "exception"
                context.errors.append(str(e))
                self.audit.log("phase_exception", {
                    "phase": phase_name, "error": str(e),
                })
                raise

        result.stats = context.stats
        result.classification = context.classification
        result.extraction = context.extraction
        result.healing = context.healing
        result.duration_sec = time.time() - t0
        return result
