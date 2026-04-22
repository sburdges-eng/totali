"""Import-smoke: every shipping module imports cleanly.

Cheap regression net — one assertion per module. If a module starts raising at
import time (broken import, eager file IO, missing optional dep without
graceful fallback), this test catches it before any other test even gets to
collection.
"""

import importlib

import pytest


PRODUCTION_MODULES = [
    "totali",
    "totali.audit.logger",
    "totali.audit.verify",
    "totali.geodetic.gatekeeper",
    "totali.segmentation.classifier",
    "totali.extraction.extractor",
    "totali.cad_shielding.shield",
    "totali.linting.surveyor_lint",
    "totali.pipeline.base_phase",
    "totali.pipeline.context",
    "totali.pipeline.models",
    "totali.pipeline.orchestrator",
    "totali.repl.contracts",
]


@pytest.mark.parametrize("modname", PRODUCTION_MODULES)
def test_module_imports(modname):
    importlib.import_module(modname)


class TestPublicSymbols:
    def test_audit_exports(self):
        from totali.audit.logger import AuditLogger, UnknownAuditEvent
        assert AuditLogger is not None
        assert UnknownAuditEvent is not None

    def test_audit_verify_exports(self):
        from totali.audit.verify import verify_log, main
        assert callable(verify_log)
        assert callable(main)

    def test_orchestrator_exports(self):
        from totali.pipeline.orchestrator import (
            PipelineOrchestrator,
            PHASE_ORDER,
            PhaseTimeout,
        )
        assert PipelineOrchestrator is not None
        assert PHASE_ORDER == ["geodetic", "segment", "extract", "shield", "lint"]
        assert issubclass(PhaseTimeout, TimeoutError)

    def test_shield_exports(self):
        from totali.cad_shielding.shield import CADShield, NonConformingLayerName
        assert CADShield is not None
        assert issubclass(NonConformingLayerName, ValueError)
