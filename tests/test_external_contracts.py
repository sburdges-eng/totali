"""Contract-shape tests for totali.external.

Verifies:
- All sibling-service output carries the non-authoritative invariant (L4L).
- Schema versions are frozen semver strings.
- Enum-like fields (format, embedding space, layer suffix) reject unknowns.
- Numeric fields respect their bounds.
- Protocol definitions exist and have the expected public methods.
"""

import inspect

import pytest
from pydantic import ValidationError

from totali.external import (
    AuracadAdapter,
    AuracadHealReport,
    AuracadReadReport,
    AuracadTransformReport,
    L4LAnomalyScore,
    L4LInferenceAdapter,
    L4LProposal,
    L4LSceneEmbedding,
)
from totali.external.auracad_contract import AURACAD_CONTRACT_VERSION
from totali.external.l4l_contract import L4L_CONTRACT_VERSION


# ------------------------------------------------------------------------
# Version pins
# ------------------------------------------------------------------------

class TestContractVersions:
    def test_auracad_contract_version_is_semver(self):
        assert AURACAD_CONTRACT_VERSION == "1.0.0"

    def test_l4l_contract_version_is_semver(self):
        assert L4L_CONTRACT_VERSION == "1.0.0"


# ------------------------------------------------------------------------
# Auracad contracts
# ------------------------------------------------------------------------

class TestAuracadReadReport:
    def test_minimum_fields_accepted(self):
        r = AuracadReadReport(file="x.dxf", format="dxf")
        assert r.schema_version == "1.0.0"
        assert r.layers == []
        assert r.errors == []

    @pytest.mark.parametrize("fmt", ["dxf", "dwg"])
    def test_known_formats(self, fmt):
        r = AuracadReadReport(file="x", format=fmt)
        assert r.format == fmt

    @pytest.mark.parametrize("bad", ["svg", "pdf", "DXF", ""])
    def test_unknown_formats_rejected(self, bad):
        with pytest.raises(ValidationError):
            AuracadReadReport(file="x", format=bad)

    def test_extra_fields_forbidden(self):
        with pytest.raises(ValidationError):
            AuracadReadReport(file="x", format="dxf", extra_key="nope")


class TestAuracadHealReport:
    def test_non_negative_counts(self):
        r = AuracadHealReport(
            input_entity_count=10, healed_count=2,
            quarantined_count=1, passed_count=7,
        )
        assert r.healed_count == 2

    @pytest.mark.parametrize("field", ["input_entity_count", "healed_count",
                                        "quarantined_count", "passed_count"])
    def test_negative_count_rejected(self, field):
        payload = {
            "input_entity_count": 0, "healed_count": 0,
            "quarantined_count": 0, "passed_count": 0,
        }
        payload[field] = -1
        with pytest.raises(ValidationError):
            AuracadHealReport(**payload)


class TestAuracadTransformReport:
    def test_basic(self):
        r = AuracadTransformReport(
            source_epsg=2231, target_epsg=4326,
            proj_version="9.4.1", point_count=500,
        )
        assert r.proj_version == "9.4.1"
        assert r.point_count == 500


# ------------------------------------------------------------------------
# L4L non-authoritative invariant (the critical one)
# ------------------------------------------------------------------------

class TestL4LAuthoritativeInvariant:
    """S-7 mirror: every L4L output must refuse authoritative=True at construction."""

    def test_scene_embedding_default_false(self):
        e = L4LSceneEmbedding(
            space="scene_global", vector_len=512,
            model_id="jepa-v1", model_sha256="a" * 64,
        )
        assert e.authoritative is False

    def test_anomaly_score_default_false(self):
        a = L4LAnomalyScore(
            target_id="obj-1", score=0.7,
            model_id="jepa-v1", model_sha256="a" * 64,
        )
        assert a.authoritative is False

    def test_proposal_default_false(self):
        p = L4LProposal(
            proposal_kind="action_hint", rationale="test", confidence=0.5,
        )
        assert p.authoritative is False

    @pytest.mark.parametrize(
        "cls,kwargs",
        [
            (L4LSceneEmbedding, dict(space="scene_global", vector_len=1,
                                      model_id="m", model_sha256="a" * 64)),
            (L4LAnomalyScore, dict(target_id="t", score=0.5,
                                    model_id="m", model_sha256="a" * 64)),
            (L4LProposal, dict(proposal_kind="action_hint",
                                rationale="r", confidence=0.5)),
        ],
    )
    @pytest.mark.parametrize("truthy", [True, 1, "yes"])
    def test_truthy_authoritative_rejected(self, cls, kwargs, truthy):
        with pytest.raises(ValidationError):
            cls(**kwargs, authoritative=truthy)


class TestL4LSceneEmbedding:
    @pytest.mark.parametrize("space", [
        "scene_global", "tile_visual", "object_semantic",
        "object_geometry", "command_context", "code_module",
    ])
    def test_known_spaces(self, space):
        e = L4LSceneEmbedding(
            space=space, vector_len=128,
            model_id="m", model_sha256="a" * 64,
        )
        assert e.space == space

    def test_unknown_space_rejected(self):
        with pytest.raises(ValidationError):
            L4LSceneEmbedding(
                space="made_up", vector_len=128,
                model_id="m", model_sha256="a" * 64,
            )

    def test_sha256_length_enforced(self):
        with pytest.raises(ValidationError):
            L4LSceneEmbedding(
                space="scene_global", vector_len=128,
                model_id="m", model_sha256="tooshort",
            )

    def test_vector_len_positive(self):
        with pytest.raises(ValidationError):
            L4LSceneEmbedding(
                space="scene_global", vector_len=0,
                model_id="m", model_sha256="a" * 64,
            )


class TestL4LAnomalyScore:
    @pytest.mark.parametrize("score", [0.0, 0.5, 1.0])
    def test_bounds(self, score):
        a = L4LAnomalyScore(
            target_id="t", score=score,
            model_id="m", model_sha256="a" * 64,
        )
        assert a.score == score

    @pytest.mark.parametrize("score", [-0.01, 1.01, 10.0, -1.0])
    def test_out_of_bounds_rejected(self, score):
        with pytest.raises(ValidationError):
            L4LAnomalyScore(
                target_id="t", score=score,
                model_id="m", model_sha256="a" * 64,
            )


class TestL4LProposal:
    def test_draft_layer_accepted(self):
        p = L4LProposal(
            proposal_kind="geometry_suggestion",
            target_layer="TOTaLi-SURV-BRKLN-DRAFT",
            rationale="ml suggestion",
            confidence=0.8,
        )
        assert p.target_layer.endswith("-DRAFT")

    def test_qa_layer_accepted(self):
        p = L4LProposal(
            proposal_kind="classification_refinement",
            target_layer="TOTaLi-QA-OCCLUSION",
            rationale="anomaly flagged",
            confidence=0.9,
        )
        assert p.target_layer.startswith("TOTaLi-QA-")

    def test_no_target_layer_accepted(self):
        p = L4LProposal(
            proposal_kind="action_hint",
            rationale="no layer required", confidence=0.5,
        )
        assert p.target_layer is None

    @pytest.mark.parametrize(
        "bad_layer",
        ["SomeLayer", "TOTaLi-SURV-BRKLN", "MyOwn-DRAFT", "TOTaLi-SURV"],
    )
    def test_non_draft_non_qa_layer_rejected(self, bad_layer):
        with pytest.raises(ValidationError):
            L4LProposal(
                proposal_kind="geometry_suggestion",
                target_layer=bad_layer,
                rationale="r", confidence=0.5,
            )

    @pytest.mark.parametrize("conf", [-0.01, 1.01])
    def test_confidence_out_of_bounds_rejected(self, conf):
        with pytest.raises(ValidationError):
            L4LProposal(
                proposal_kind="action_hint", rationale="r", confidence=conf,
            )


# ------------------------------------------------------------------------
# Protocol surfaces
# ------------------------------------------------------------------------

class TestProtocols:
    def test_auracad_adapter_has_expected_methods(self):
        expected = {"read_cad", "heal_geometry", "transform_crs"}
        actual = {
            n for n, m in inspect.getmembers(AuracadAdapter, inspect.isfunction)
            if not n.startswith("_")
        }
        assert expected <= actual, f"missing: {expected - actual}"

    def test_l4l_adapter_has_expected_methods(self):
        expected = {"embed_scene", "score_anomalies", "propose_actions"}
        actual = {
            n for n, m in inspect.getmembers(L4LInferenceAdapter, inspect.isfunction)
            if not n.startswith("_")
        }
        assert expected <= actual, f"missing: {expected - actual}"
