"""Phase 2 (coded survey): deterministic field-code classification."""

from __future__ import annotations

import os

from totali.audit.logger import AuditLogger
from totali.fieldcodes import DESCRIPTIONS_PATH_ENV, FLD_PATH_ENV, load_field_codes
from totali.pipeline.base_phase import PipelinePhase
from totali.pipeline.context import PipelineContext
from totali.pipeline.models import CodedSurveyPoint, PhaseResult
from totali.segmentation.coded import classify_coded_points, draft_layer_counts
from totali.segmentation.dataset import LabeledPoint, parse_asc


class CodedPointClassifier(PipelinePhase):
    """Classify coded survey exports via the field-code taxonomy (authoritative)."""

    phase_name = "segment"

    def __init__(self, config: dict, audit: AuditLogger):
        super().__init__(config, audit)
        self.discipline = config.get("discipline", "SURV")

    def validate_inputs(self, context: PipelineContext) -> tuple[bool, list[str]]:
        errors: list[str] = []
        if context.input_kind != "coded_survey":
            errors.append("input_kind must be coded_survey")
        coded = context.extras.get("coded_points")
        if not coded and context.points_xyz is None:
            errors.append("coded_points missing; run geodetic phase first")
        return len(errors) == 0, errors

    def _load_points(self, context: PipelineContext) -> list[LabeledPoint]:
        cached = context.extras.get("coded_points")
        if cached:
            return cached
        table = self._field_code_table()
        return parse_asc(context.input_path, table)

    def _field_code_table(self):
        fld = self.config.get("fieldcode_fld") or os.environ.get(FLD_PATH_ENV)
        if not fld:
            raise RuntimeError(
                f"set segmentation.fieldcode_fld or {FLD_PATH_ENV} for coded survey input"
            )
        desc = self.config.get("fieldcode_descriptions") or os.environ.get(
            DESCRIPTIONS_PATH_ENV
        )
        return load_field_codes(fld, desc)

    def run(self, context: PipelineContext) -> PhaseResult:
        points = self._load_points(context)
        if not points:
            return PhaseResult(
                phase="segment",
                success=False,
                message="No coded survey points parsed from input",
            )

        classifications = classify_coded_points(points, discipline=self.discipline)
        layer_counts = draft_layer_counts(classifications)

        survey_points = [
            CodedSurveyPoint(
                point_id=c.point_id,
                x=p.easting,
                y=p.northing,
                z=p.elevation,
                draft_layer=c.draft_layer,
                field_code=c.field_code,
                firm_layer=c.firm_layer,
            )
            for p, c in zip(points, classifications)
        ]

        self.audit.log("classify_coded", {
            "point_count": len(points),
            "labeled_count": sum(1 for p in points if p.is_labeled),
            "draft_layer_counts": layer_counts,
            "authoritative": True,
        })

        return PhaseResult(
            phase="segment",
            success=True,
            message=(
                f"Classified {len(points)} coded points "
                f"({sum(1 for p in points if p.is_labeled)} labeled)"
            ),
            data={
                "coded_survey_points": survey_points,
                "input_kind": "coded_survey",
            },
        )
