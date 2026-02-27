"""
Typed pipeline context and configuration models.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import numpy as np
from pydantic import BaseModel, ConfigDict, Field

from totali.pipeline.models import (
    CRSMetadata,
    ClassificationResult,
    ExtractionResult,
    HealingReport,
    LintItem,
    PointCloudStats,
    SurveyData,
)


class ProjectConfig(BaseModel):
    name: str = "unknown"
    version: str = "0.1.0"
    pls_authority: str | None = None


class AuditConfig(BaseModel):
    log_dir: str = "audit_logs"
    log_format: str = "jsonl"
    hash_algorithm: str = "sha256"
    log_events: list[str] = Field(default_factory=list)


class PipelineConfig(BaseModel):
    model_config = ConfigDict(extra="allow")

    project: ProjectConfig = Field(default_factory=ProjectConfig)
    geodetic: dict[str, Any] = Field(default_factory=dict)
    segmentation: dict[str, Any] = Field(default_factory=dict)
    extraction: dict[str, Any] = Field(default_factory=dict)
    cad_shielding: dict[str, Any] = Field(default_factory=dict)
    linting: dict[str, Any] = Field(default_factory=dict)
    integration: dict[str, Any] = Field(default_factory=dict)
    audit: AuditConfig = Field(default_factory=AuditConfig)


class PipelineContext(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True, validate_assignment=False)

    input_path: str
    output_dir: Path

    points_xyz: np.ndarray | None = None
    las: Any = None
    crs: CRSMetadata | None = None
    stats: PointCloudStats | None = None
    input_hash: str | None = None
    classification: ClassificationResult | None = None
    extraction: ExtractionResult | None = None
    survey_data: SurveyData | None = None
    dxf_path: str | None = None
    manifest: dict[str, Any] | None = None
    healing: HealingReport | None = None
    lint_items: list[LintItem] = Field(default_factory=list)
    lint_report: dict[str, Any] | None = None

    last_output_files: list[Path] = Field(default_factory=list)
    phase_status: dict[str, str] = Field(default_factory=dict)
    errors: list[str] = Field(default_factory=list)
    extras: dict[str, Any] = Field(default_factory=dict)

    def merge_data(self, data: dict[str, Any]) -> None:
        for key, value in data.items():
            if hasattr(self, key):
                setattr(self, key, value)
            else:
                self.extras[key] = value
