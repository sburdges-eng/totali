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
from totali.geodetic.gatekeeper import GeodeticGatekeeper
from totali.segmentation.classifier import PointCloudClassifier
from totali.extraction.extractor import DeterministicExtractor
from totali.linting.surveyor_lint import SurveyorLinter
from totali.audit.logger import AuditLogger


PHASE_ORDER = ["geodetic", "segment", "extract", "shield", "lint"]


class PipelineOrchestrator:
    def __init__(self, config: dict, audit: AuditLogger, output_dir: Path):
        self.config = PipelineConfig.model_validate(config)
        self.audit = audit
        self.output_dir = output_dir

        from totali.cad_shielding.shield import CADShield

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
            crs=self.config.integration.source_crs
        )

        for p_name in phases_to_run:
            if p_name not in self.phases:
                continue

            phase_obj = self.phases[p_name]
            valid, errors = phase_obj.validate_inputs(context)
            if not valid:
                return PipelineResult(
                    project_id=self.config.project.name,
                    success=False,
                    phases=[PhaseResult(phase=p_name, success=False, message="; ".join(errors))]
                )

            p_res = phase_obj.run(context)
            result.phases.append(p_res)

            if not p_res.success:
                result.success = False
                break

            context.merge_data(p_res.data)

        result.duration_sec = time.time() - t0
        return result
