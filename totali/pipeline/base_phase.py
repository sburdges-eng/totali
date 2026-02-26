"""
Shared phase contract for pipeline processors.
"""

from __future__ import annotations

from abc import ABC, abstractmethod

from totali.audit.logger import AuditLogger
from totali.pipeline.context import PipelineContext
from totali.pipeline.models import PhaseResult


class PipelinePhase(ABC):
    phase_name: str = "phase"

    def __init__(self, config: dict, audit: AuditLogger):
        self.config = config or {}
        self.audit = audit
        self.validate_config()

    @abstractmethod
    def run(self, context: PipelineContext) -> PhaseResult:
        raise NotImplementedError

    def validate_config(self) -> None:
        return None

    def validate_inputs(self, context: PipelineContext) -> tuple[bool, list[str]]:
        return True, []

    def get_required_inputs(self) -> set[str]:
        return set()

    def get_provided_outputs(self) -> set[str]:
        return set()
