"""
Pipeline Data Models
====================
Shared types across all pipeline phases.
"""

from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Optional
import numpy as np


class GeometryStatus(str, Enum):
    DRAFT = "DRAFT"
    FLAGGED = "FLAGGED"
    ACCEPTED = "ACCEPTED"
    REJECTED = "REJECTED"
    CERTIFIED = "CERTIFIED"


class OcclusionType(str, Enum):
    NONE = "none"
    CANOPY = "canopy"
    STRUCTURE = "structure"
    SHADOW = "shadow"
    UNKNOWN = "unknown"


@dataclass
class CRSMetadata:
    epsg_code: int
    epoch: Optional[str] = None
    geoid_model: Optional[str] = None
    horizontal_unit: str = "US_survey_foot"
    vertical_unit: str = "US_survey_foot"
    source_datum: Optional[str] = None
    is_valid: bool = False
    validation_errors: list = field(default_factory=list)


@dataclass
class PointCloudStats:
    point_count: int = 0
    bounds_min: Optional[np.ndarray] = None
    bounds_max: Optional[np.ndarray] = None
    has_rgb: bool = False
    has_intensity: bool = False
    has_classification: bool = False
    source_file: Optional[str] = None
    crs: Optional[CRSMetadata] = None


@dataclass
class ClassificationResult:
    """Per-point classification with confidence scores."""
    labels: Optional[np.ndarray] = None
    confidences: Optional[np.ndarray] = None
    occlusion_mask: Optional[np.ndarray] = None
    class_counts: dict = field(default_factory=dict)
    mean_confidence: float = 0.0
    low_confidence_count: int = 0
    occluded_count: int = 0


@dataclass
class ExtractionResult:
    """Deterministic geometry extraction outputs."""
    dtm_vertices: Optional[np.ndarray] = None
    dtm_faces: Optional[np.ndarray] = None
    breaklines: list = field(default_factory=list)
    contours_minor: list = field(default_factory=list)
    contours_index: list = field(default_factory=list)
    building_footprints: list = field(default_factory=list)
    curb_lines: list = field(default_factory=list)
    wire_lines: list = field(default_factory=list)
    hardscape_polygons: list = field(default_factory=list)
    occlusion_zones: list = field(default_factory=list)
    error_metrics: dict = field(default_factory=dict)
    qa_flags: list = field(default_factory=list)


@dataclass
class HealingReport:
    """Geometry healing/quarantine results."""
    input_entity_count: int = 0
    healed_count: int = 0
    quarantined_count: int = 0
    passed_count: int = 0
    issues: list = field(default_factory=list)


@dataclass
class PhaseResult:
    phase: str
    success: bool
    duration_sec: float = 0.0
    message: str = ""
    data: dict = field(default_factory=dict)
    output_files: list = field(default_factory=list)


@dataclass
class PipelineResult:
    project_id: str
    phases: list = field(default_factory=list)
    output_files: list = field(default_factory=list)
    duration_sec: float = 0.0
    success: bool = True
    stats: Optional[PointCloudStats] = None
    classification: Optional[ClassificationResult] = None
    extraction: Optional[ExtractionResult] = None
    healing: Optional[HealingReport] = None


@dataclass
class LintItem:
    """A single suggestion for surveyor review."""
    item_id: str
    geometry_type: str
    layer: str
    status: GeometryStatus = GeometryStatus.DRAFT
    confidence: float = 0.0
    occlusion: OcclusionType = OcclusionType.NONE
    source_hash: str = ""
    reviewer: Optional[str] = None
    review_timestamp: Optional[str] = None
    notes: str = ""
