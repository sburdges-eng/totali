"""Contract surface for TOTaLi ↔ L4L integration.

L4L is the generative/simulation/JEPA layer — **advisory only** per the
production-design invariant (JEPA output is never authoritative).

TOTaLi's potential consumption paths:

1. JEPA scene embeddings for retrieval / anomaly ranking at surveyor-lint
   time (augments `SurveyorLinter._confidence_color` with ML signal).
2. L4L's generative proposals for CAD objects — surfaced as DRAFT-layer
   suggestions, never auto-applied.

This module defines the contract only. Every output carries
`authoritative=False` with a constructor guard; truthy assignment raises at
construction, mirroring `ClassificationResult.authoritative` (S-7).

See `~/Dev/liberals4liberty/docs/PRODUCTION_DESIGN_ALIGNMENT.md` for L4L's
side of the contract.
"""

from __future__ import annotations

from typing import Any, Protocol

from pydantic import BaseModel, ConfigDict, Field, field_validator


# Frozen contract version. Bump MAJOR when breaking; MINOR for additive fields.
L4L_CONTRACT_VERSION = "1.0.0"


class _NonAuthoritativeBase(BaseModel):
    """Base class for all L4L outputs. Enforces §1.3 invariant:
    authoritative is False, always. A truthy value raises at construction.
    """

    model_config = ConfigDict(extra="forbid")

    schema_version: str = Field(default=L4L_CONTRACT_VERSION)
    authoritative: bool = False

    @field_validator("authoritative")
    @classmethod
    def _must_be_false(cls, v: bool) -> bool:
        if v is not False:
            raise ValueError(
                "L4L output is never authoritative. TOTaLi §1.3 invariant: "
                "ML output lands only on DRAFT layers and requires PLS review."
            )
        return v


class L4LSceneEmbedding(_NonAuthoritativeBase):
    """A scene-level embedding produced by L4L's JEPA.

    `space` indicates which embedding space (per the design:
    scene_global / tile_visual / object_semantic / object_geometry /
    command_context / code_module).
    `vector` is unit-normalized; dimension pinned per space at adapter level.
    """

    space: str
    vector_len: int = Field(ge=1)
    model_id: str
    model_sha256: str = Field(min_length=64, max_length=64)

    @field_validator("space")
    @classmethod
    def _known_space(cls, v: str) -> str:
        allowed = {
            "scene_global",
            "tile_visual",
            "object_semantic",
            "object_geometry",
            "command_context",
            "code_module",
        }
        if v not in allowed:
            raise ValueError(f"unknown embedding space {v!r}; allowed: {sorted(allowed)}")
        return v


class L4LAnomalyScore(_NonAuthoritativeBase):
    """Anomaly score for a scene region or object.

    Score is bounded [0, 1]. Use as an additional input to surveyor-lint
    confidence coloring; never as a promotion gate.
    """

    target_id: str  # scene object / tile / classification-segment ID
    score: float = Field(ge=0.0, le=1.0)
    model_id: str
    model_sha256: str = Field(min_length=64, max_length=64)


class L4LProposal(_NonAuthoritativeBase):
    """A generative proposal from L4L for an action or geometry addition.

    Emitted as a ranked candidate for the orchestrator's next-best-action
    ranking. TOTaLi surfaces top-k as DRAFT-layer suggestions; surveyors
    accept/reject/defer per L-5.
    """

    proposal_kind: str  # "geometry_suggestion" | "classification_refinement" | "action_hint"
    target_layer: str | None = None  # must end in -DRAFT if set
    rationale: str
    confidence: float = Field(ge=0.0, le=1.0)
    payload: dict[str, Any] = Field(default_factory=dict)

    @field_validator("target_layer")
    @classmethod
    def _must_be_draft_or_qa(cls, v: str | None) -> str | None:
        if v is None:
            return v
        # TOTaLi §1.3 invariant mirror: layer names must match the canonical
        # pattern TOTaLi-<DISC>-<FEAT>-DRAFT or the TOTaLi-QA-* exemption.
        if v.startswith("TOTaLi-QA-"):
            return v
        if v.startswith("TOTaLi-") and v.endswith("-DRAFT"):
            return v
        raise ValueError(
            f"target_layer={v!r} must be TOTaLi-<DISC>-<FEAT>-DRAFT or TOTaLi-QA-*"
        )


class L4LInferenceAdapter(Protocol):
    """The runtime contract an L4L inference adapter must satisfy.

    Implementations (future feature branches) may be:
    - ONNX runtime loading via `totali.models.loader.load_onnx` (preferred)
    - Subprocess RPC to L4L-Intelligence service
    - In-process pybind11 binding

    Whatever the transport, every output is a non-authoritative Pydantic model.
    """

    def embed_scene(self, context: dict[str, Any]) -> L4LSceneEmbedding:
        """Produce a `scene_global` embedding for the given context."""
        ...

    def score_anomalies(self, targets: list[str]) -> list[L4LAnomalyScore]:
        """Score a batch of targets [0,1]. No side effects beyond the model call."""
        ...

    def propose_actions(self, context: dict[str, Any], top_k: int = 5) -> list[L4LProposal]:
        """Return up to top_k ranked proposals. Top_k capped at adapter-level
        limit (suggestion: 10). Never promotes geometry to certified."""
        ...
