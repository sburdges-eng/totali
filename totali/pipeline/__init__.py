from totali.pipeline.models import PipelineResult, PhaseResult, GeometryStatus
from totali.pipeline.base_phase import PipelinePhase
from totali.pipeline.context import PipelineConfig, PipelineContext


def __getattr__(name: str):
    if name == "PipelineOrchestrator":
        from totali.pipeline.orchestrator import PipelineOrchestrator
        return PipelineOrchestrator
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "PipelineOrchestrator",
    "PipelinePhase",
    "PipelineConfig",
    "PipelineContext",
    "PipelineResult",
    "PhaseResult",
    "GeometryStatus",
]
