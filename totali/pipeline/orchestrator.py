"""
Pipeline Orchestrator
=====================
Runs phases in sequence, enforcing the division of labor:
  AI Classifies → Algorithms Measure → Humans Certify
"""

import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError
from pathlib import Path
from typing import Optional

from totali.pipeline.models import PipelineResult, PhaseResult
from totali.pipeline.context import PipelineConfig, PipelineContext
from totali.audit.logger import AuditLogger


PHASE_ORDER = ["geodetic", "segment", "extract", "shield", "lint"]


class PhaseTimeout(TimeoutError):
    """P-6: raised when a phase exceeds its configured timeout budget."""


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
            self.audit.log("phase_start", {"phase": phase_name})

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

                timeout = self._phase_timeout(phase_name)
                phase_result = self._run_with_timeout(processor, context, timeout)
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

            except PhaseTimeout as e:
                phase_result = PhaseResult(
                    phase=phase_name, success=False,
                    duration_sec=time.time() - pt0,
                    message=f"Phase exceeded timeout: {e}",
                )
                result.phases.append(phase_result)
                result.success = False
                context.phase_status[phase_name] = "timeout"
                context.errors.append(str(e))
                self.audit.log("phase_timeout", {
                    "phase": phase_name,
                    "timeout_sec": self._phase_timeout(phase_name),
                    "elapsed_sec": time.time() - pt0,
                })
                break

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

    def _phase_timeout(self, phase_name: str) -> Optional[float]:
        """P-6: resolve per-phase timeout. Order:
        1. `<phase_section>.timeout_sec` explicit override
        2. `cad_shielding.middleware_timeout_sec` for shield (legacy field)
        3. None (no enforcement)
        """
        section_map = {
            "geodetic": self.config.geodetic,
            "segment": self.config.segmentation,
            "extract": self.config.extraction,
            "shield": self.config.cad_shielding,
            "lint": self.config.linting,
        }
        section = section_map.get(phase_name) or {}
        explicit = section.get("timeout_sec")
        if explicit is not None:
            return float(explicit)
        if phase_name == "shield":
            mw = section.get("middleware_timeout_sec")
            if mw is not None:
                return float(mw)
        return None

    def _run_with_timeout(
        self,
        processor,
        context: PipelineContext,
        timeout: Optional[float],
    ) -> PhaseResult:
        """P-6: execute processor.run(context) with optional wall-clock timeout.

        timeout=None → direct call. Positive timeout → ThreadPoolExecutor with
        future.result(timeout=t); on FuturesTimeoutError raise PhaseTimeout so
        the outer handler emits phase_timeout and halts.
        """
        if timeout is None or timeout <= 0:
            return processor.run(context)
        with ThreadPoolExecutor(max_workers=1) as ex:
            future = ex.submit(processor.run, context)
            try:
                return future.result(timeout=timeout)
            except FuturesTimeoutError as e:
                raise PhaseTimeout(
                    f"phase did not return within {timeout:.3f}s"
                ) from e
