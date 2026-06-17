"""
Pipeline Data Models
====================
Shared types across all pipeline phases.
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Optional
import numpy as np


class GeometryStatus(str, Enum):
    DRAFT = "DRAFT"
    FLAGGED = "FLAGGED"
    ACCEPTED = "ACCEPTED"
    REJECTED = "REJECTED"
    DEFERRED = "DEFERRED"  # L-5: surveyor deferred review to next session
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
    """Per-point classification with confidence scores.

    S-7: Always non-authoritative. ML output never promotes to certified
    geometry. Downstream phases check `authoritative` and refuse to treat
    this as ground truth — the only legal sink is the DRAFT layer + PLS loop.
    """
    labels: Optional[np.ndarray] = None         # int array of class IDs
    confidences: Optional[np.ndarray] = None     # float array [0,1]
    occlusion_mask: Optional[np.ndarray] = None  # bool array
    class_counts: dict = field(default_factory=dict)
    mean_confidence: float = 0.0
    low_confidence_count: int = 0
    occluded_count: int = 0
    # S-7 TOTaLi §1.3 invariant: classifier output is probabilistic, never
    # authoritative. Default False; __post_init__ refuses any truthy value.
    authoritative: bool = False

    def __post_init__(self):
        if self.authoritative is not False:
            raise ValueError(
                "ClassificationResult.authoritative must be False — "
                "ML output is never authoritative in TOTaLi (§1.3)."
            )


@dataclass(frozen=True)
class CodedSurveyPoint:
    """One authoritative survey shot for coded-export → DXF placement."""

    point_id: str
    x: float
    y: float
    z: float
    draft_layer: str
    field_code: str
    firm_layer: Optional[str] = None


@dataclass
class ExtractionResult:
    """Deterministic geometry extraction outputs."""
    coded_survey_points: list = field(default_factory=list)
    dtm_vertices: Optional[np.ndarray] = None
    dtm_faces: Optional[np.ndarray] = None
    breaklines: list = field(default_factory=list)       # list of Nx3 arrays
    contours_minor: list = field(default_factory=list)    # list of Nx2 arrays
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
    geometry_type: str           # "breakline", "contour", "building", etc.
    layer: str
    status: GeometryStatus = GeometryStatus.DRAFT
    confidence: float = 0.0
    occlusion: OcclusionType = OcclusionType.NONE
    source_hash: str = ""
    reviewer: Optional[str] = None
    review_timestamp: Optional[str] = None
    notes: str = ""
