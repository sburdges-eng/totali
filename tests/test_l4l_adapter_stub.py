"""Tests for :class:`totali.external.l4l_adapter_stub.StubL4LInferenceAdapter`.

Phase 2 of the L4L integration roll-out: a local, deterministic stub that
satisfies the :class:`L4LInferenceAdapter` Protocol so TOTaLi-side callers
can be built and regression-tested before the real L4L-Intelligence ONNX
service lands.

These tests assert:

* the stub structurally satisfies the :class:`L4LInferenceAdapter`
  Protocol (the Protocol is not ``@runtime_checkable``, same as
  ``AuracadAdapter``) — we probe each method by attribute + callability
  rather than ``isinstance``,
* ``embed_scene`` returns a well-formed :class:`L4LSceneEmbedding`,
* ``score_anomalies`` is pure (deterministic per target_id) and bounded,
* ``propose_actions`` respects the ``top_k`` contract (0, negative, and
  over-10 all clamp), marks every proposal as stub-origin via the
  ``rationale`` field, and is deterministic per context,
* the non-authoritative invariant holds across every output.
"""

from __future__ import annotations

import pytest

from totali.external.l4l_adapter_stub import StubL4LInferenceAdapter
from totali.external.l4l_contract import (
    L4LAnomalyScore,
    L4LInferenceAdapter,
    L4LProposal,
    L4LSceneEmbedding,
)

_ALLOWED_PROPOSAL_KINDS = {
    "geometry_suggestion",
    "classification_refinement",
    "action_hint",
}


@pytest.fixture
def adapter() -> StubL4LInferenceAdapter:
    return StubL4LInferenceAdapter()


# ===================================================================
# 1. Protocol conformance
# ===================================================================


class TestProtocolConformance:
    """``StubL4LInferenceAdapter`` structurally satisfies
    :class:`L4LInferenceAdapter`.

    The Protocol is not decorated ``@runtime_checkable`` — ``isinstance``
    against a bare Protocol raises ``TypeError``. We instead probe each
    method name for presence + callability.
    """

    _PROTOCOL_METHODS = ("embed_scene", "score_anomalies", "propose_actions")

    def test_instance_exposes_all_protocol_methods(self, adapter):
        for name in self._PROTOCOL_METHODS:
            attr = getattr(adapter, name, None)
            assert attr is not None, f"missing Protocol method {name!r}"
            assert callable(attr), f"{name!r} is not callable"

    def test_protocol_itself_declares_expected_methods(self):
        for name in self._PROTOCOL_METHODS:
            assert hasattr(L4LInferenceAdapter, name), (
                f"L4LInferenceAdapter Protocol lost method {name!r}"
            )

    def test_isinstance_with_runtime_checkable_would_require_decorator(self):
        # Pins current non-runtime-checkable status. If someone later adds
        # @runtime_checkable to L4LInferenceAdapter, this test will start
        # failing and signal that conformance probing here should be
        # upgraded to a real ``isinstance`` check.
        with pytest.raises(TypeError):
            isinstance(StubL4LInferenceAdapter(), L4LInferenceAdapter)


# ===================================================================
# 2. embed_scene
# ===================================================================


class TestEmbedScene:
    def test_returns_scene_embedding(self, adapter):
        emb = adapter.embed_scene({"tile_id": "t1"})
        assert isinstance(emb, L4LSceneEmbedding)

    def test_space_is_scene_global(self, adapter):
        emb = adapter.embed_scene({"tile_id": "t1"})
        assert emb.space == "scene_global"

    def test_vector_len_positive(self, adapter):
        emb = adapter.embed_scene({"tile_id": "t1"})
        assert emb.vector_len > 0

    def test_non_authoritative(self, adapter):
        emb = adapter.embed_scene({"tile_id": "t1"})
        assert emb.authoritative is False

    def test_model_sha256_is_64_hex_chars(self, adapter):
        emb = adapter.embed_scene({"tile_id": "t1"})
        assert len(emb.model_sha256) == 64
        # All hex digits (str.isalnum is too loose; check explicitly).
        assert all(c in "0123456789abcdef" for c in emb.model_sha256)

    def test_deterministic_across_calls(self, adapter):
        """Same adapter instance, same context, twice → identical embedding."""
        ctx = {"tile_id": "t1", "extent": [0, 0, 100, 100]}
        e1 = adapter.embed_scene(ctx)
        e2 = adapter.embed_scene(ctx)
        assert e1.model_sha256 == e2.model_sha256
        assert e1.space == e2.space
        assert e1.vector_len == e2.vector_len

    def test_deterministic_across_instances(self):
        """Fresh adapters produce the same embedding for the same context.

        Note: the stub is currently context-blind — embedding fields do not
        vary with `context`. This test pins that property intentionally so
        cross-instance determinism is guaranteed, AND so a future real
        adapter that starts responding to context cannot silently regress
        this property without updating the test.
        """
        a1 = StubL4LInferenceAdapter()
        a2 = StubL4LInferenceAdapter()
        ctx = {"tile_id": "shared"}
        e1 = a1.embed_scene(ctx)
        e2 = a2.embed_scene(ctx)
        assert e1.model_sha256 == e2.model_sha256
        assert e1.space == e2.space
        assert e1.vector_len == e2.vector_len
        assert e1.model_id == e2.model_id

    def test_context_blind_is_documented_stub_limitation(self, adapter):
        """Documented stub limitation: embedding fields do not vary with
        context. The real L4L service will produce context-dependent
        embeddings; this test pins the current stub behavior so the
        limitation is obvious and the next adapter version must update
        this test deliberately."""
        e1 = adapter.embed_scene({"tile_id": "a"})
        e2 = adapter.embed_scene({"tile_id": "b", "extent": [1, 2, 3, 4]})
        # Same pinned fields — stub is context-blind by design.
        assert e1.model_sha256 == e2.model_sha256
        assert e1.vector_len == e2.vector_len
        assert e1.space == e2.space


# ===================================================================
# 3. score_anomalies
# ===================================================================


class TestScoreAnomalies:
    def test_returns_one_per_target(self, adapter):
        scores = adapter.score_anomalies(["obj-1", "obj-2", "obj-3"])
        assert len(scores) == 3
        for s in scores:
            assert isinstance(s, L4LAnomalyScore)

    def test_all_non_authoritative(self, adapter):
        scores = adapter.score_anomalies(["obj-1", "obj-2", "obj-3"])
        for s in scores:
            assert s.authoritative is False

    def test_all_bounded_zero_to_one(self, adapter):
        scores = adapter.score_anomalies(["obj-1", "obj-2", "obj-3"])
        for s in scores:
            assert 0.0 <= s.score <= 1.0

    def test_empty_targets_returns_empty(self, adapter):
        assert adapter.score_anomalies([]) == []

    def test_deterministic_across_calls(self, adapter):
        first = adapter.score_anomalies(["obj-1", "obj-2", "obj-3"])
        second = adapter.score_anomalies(["obj-1", "obj-2", "obj-3"])
        assert [s.score for s in first] == [s.score for s in second]
        assert [s.target_id for s in first] == [s.target_id for s in second]

    def test_deterministic_across_instances(self):
        a = StubL4LInferenceAdapter()
        b = StubL4LInferenceAdapter()
        sa = a.score_anomalies(["x", "y", "z"])
        sb = b.score_anomalies(["x", "y", "z"])
        assert [s.score for s in sa] == [s.score for s in sb]

    def test_target_id_preserved(self, adapter):
        scores = adapter.score_anomalies(["alpha", "beta"])
        assert [s.target_id for s in scores] == ["alpha", "beta"]


# ===================================================================
# 4. propose_actions
# ===================================================================


class TestProposeActions:
    def test_default_top_k_is_five(self, adapter):
        proposals = adapter.propose_actions({"tile_id": "t1"})
        assert len(proposals) == 5

    def test_all_are_l4l_proposals(self, adapter):
        proposals = adapter.propose_actions({"tile_id": "t1"})
        for p in proposals:
            assert isinstance(p, L4LProposal)

    def test_top_k_zero_returns_empty(self, adapter):
        assert adapter.propose_actions({"tile_id": "t1"}, top_k=0) == []

    def test_top_k_negative_returns_empty(self, adapter):
        assert adapter.propose_actions({"tile_id": "t1"}, top_k=-1) == []
        assert adapter.propose_actions({"tile_id": "t1"}, top_k=-100) == []

    def test_top_k_above_ten_capped(self, adapter):
        assert len(adapter.propose_actions({"tile_id": "t1"}, top_k=11)) == 10
        assert len(adapter.propose_actions({"tile_id": "t1"}, top_k=1000)) == 10

    def test_top_k_exactly_ten_returns_ten(self, adapter):
        assert len(adapter.propose_actions({"tile_id": "t1"}, top_k=10)) == 10

    def test_all_non_authoritative(self, adapter):
        proposals = adapter.propose_actions({"tile_id": "t1"}, top_k=10)
        for p in proposals:
            assert p.authoritative is False

    def test_target_layer_shape(self, adapter):
        # The Pydantic validator on L4LProposal already rejects malformed
        # target_layer values at construction, so merely getting here proves
        # each one is valid. We re-check to make the invariant explicit.
        proposals = adapter.propose_actions({"tile_id": "t1"}, top_k=10)
        for p in proposals:
            if p.target_layer is None:
                continue
            assert (
                p.target_layer.startswith("TOTaLi-QA-")
                or (
                    p.target_layer.startswith("TOTaLi-")
                    and p.target_layer.endswith("-DRAFT")
                )
            ), f"unexpected target_layer={p.target_layer!r}"

    def test_rationale_marks_stub_origin(self, adapter):
        proposals = adapter.propose_actions({"tile_id": "t1"}, top_k=10)
        for p in proposals:
            assert "stub" in p.rationale.lower(), (
                f"rationale must flag stub origin; got {p.rationale!r}"
            )

    def test_proposal_kind_in_allowed_set(self, adapter):
        proposals = adapter.propose_actions({"tile_id": "t1"}, top_k=10)
        for p in proposals:
            assert p.proposal_kind in _ALLOWED_PROPOSAL_KINDS

    def test_confidence_bounded(self, adapter):
        proposals = adapter.propose_actions({"tile_id": "t1"}, top_k=10)
        for p in proposals:
            assert 0.0 <= p.confidence <= 1.0

    def test_deterministic_same_context(self, adapter):
        ctx = {"tile_id": "t1", "scene": "alpha"}
        first = adapter.propose_actions(ctx, top_k=5)
        second = adapter.propose_actions(ctx, top_k=5)
        assert [
            (p.proposal_kind, p.confidence, p.target_layer, p.rationale)
            for p in first
        ] == [
            (p.proposal_kind, p.confidence, p.target_layer, p.rationale)
            for p in second
        ]

    def test_deterministic_across_instances(self):
        a = StubL4LInferenceAdapter()
        b = StubL4LInferenceAdapter()
        ctx = {"tile_id": "t42"}
        pa = a.propose_actions(ctx, top_k=7)
        pb = b.propose_actions(ctx, top_k=7)
        assert [
            (p.proposal_kind, p.confidence, p.target_layer) for p in pa
        ] == [
            (p.proposal_kind, p.confidence, p.target_layer) for p in pb
        ]


# ===================================================================
# 5. Non-authoritative invariant
# ===================================================================


class TestNonAuthoritativeInvariant:
    """Spot-check the §1.3 invariant at adapter-output level.

    Every L4L output model already enforces ``authoritative=False`` via a
    field validator, but this test pins the behaviour at the adapter
    boundary so any future "improvement" that slips a truthy value past
    the validator (e.g. by bypassing Pydantic construction) trips here.
    """

    def test_embedding_is_non_authoritative(self, adapter):
        emb = adapter.embed_scene({"tile_id": "t1"})
        assert emb.authoritative is False

    def test_anomaly_scores_are_non_authoritative(self, adapter):
        for s in adapter.score_anomalies(["obj-1"]):
            assert s.authoritative is False

    def test_proposals_are_non_authoritative(self, adapter):
        for p in adapter.propose_actions({"tile_id": "t1"}, top_k=3):
            assert p.authoritative is False
