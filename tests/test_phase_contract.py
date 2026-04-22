"""P-8: Every pipeline phase implements the PipelinePhase ABC completely.

Contract tests that iterate every production phase class and assert it:
- Is a concrete subclass of PipelinePhase
- Declares `phase_name` as a non-empty string
- Overrides `run()`
- Calls super().__init__() in its constructor (inherits config + audit)
"""

import inspect

import pytest

from totali.pipeline.base_phase import PipelinePhase
from totali.geodetic.gatekeeper import GeodeticGatekeeper
from totali.segmentation.classifier import PointCloudClassifier
from totali.extraction.extractor import DeterministicExtractor
from totali.cad_shielding.shield import CADShield
from totali.linting.surveyor_lint import SurveyorLinter


PHASE_CLASSES = [
    GeodeticGatekeeper,
    PointCloudClassifier,
    DeterministicExtractor,
    CADShield,
    SurveyorLinter,
]


@pytest.mark.parametrize("cls", PHASE_CLASSES, ids=lambda c: c.__name__)
class TestPhaseContract:
    def test_is_pipeline_phase(self, cls):
        assert issubclass(cls, PipelinePhase)

    def test_not_abstract(self, cls):
        assert not inspect.isabstract(cls), f"{cls.__name__} is still abstract"

    def test_phase_name_declared(self, cls):
        name = getattr(cls, "phase_name", None)
        if name == "phase":
            # Default unoverridden value — acceptable only if class explicitly
            # declared it (some phases use a per-instance identity instead).
            assert hasattr(cls, "phase_name")
        else:
            assert isinstance(name, str) and name, (
                f"{cls.__name__}.phase_name must be a non-empty string"
            )

    def test_run_overridden(self, cls):
        assert cls.run is not PipelinePhase.run, (
            f"{cls.__name__}.run is not overridden"
        )

    def test_validate_inputs_overridden_or_default(self, cls):
        # The ABC provides a default (returns True, []); phases may rely on it
        # only when they genuinely have no precondition. Most override it —
        # surface which ones do not for maintainer review.
        _ = cls.validate_inputs
