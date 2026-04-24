"""Local deterministic stub for ``L4LInferenceAdapter`` (Phase 2).

This is the first concrete implementation of the
:class:`~totali.external.l4l_contract.L4LInferenceAdapter` Protocol. It is
**not** a real JEPA — it exists so TOTaLi-side callers (surveyor-lint ML
augmentation, draft-layer proposal surfacing, next-best-action ranking)
can be written and regression-tested against a well-formed contract
surface before the real L4L-Intelligence ONNX service lands.

Design requirements:

* Every method is deterministic — same input, bit-identical output, across
  calls and across fresh adapter instances. No wall-clock, no unseeded
  RNG. ``hashlib.sha256`` is the determinism primitive.
* Every output inherits from
  :class:`~totali.external.l4l_contract._NonAuthoritativeBase`, so any
  attempt to mark an artifact authoritative trips a Pydantic validator at
  construction time.
* ``rationale`` on every proposal contains the substring ``stub`` so
  downstream consumers can reliably distinguish stub-origin artifacts
  from real-model output during integration testing.

When the real L4L-Intelligence service lands (ONNX runtime or subprocess
RPC), it will implement the same ``L4LInferenceAdapter`` Protocol;
callers will not need to change. This adapter is intentionally **not**
exported from :mod:`totali.external.__init__` — that module is the
frozen contract-surface index. Consumers import this concrete class
explicitly.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any

from totali.external.l4l_contract import (
    L4LAnomalyScore,
    L4LProposal,
    L4LSceneEmbedding,
)

# ---------------------------------------------------------------------------
# Module-level stub-model identity
# ---------------------------------------------------------------------------
#
# The stub advertises a fixed model_id / model_sha256 pair so callers see a
# stable "which model did this come from?" signal. The sha256 is the hash of
# a constant sentinel string — this is hardcoded (by being derived from a
# constant at import time) so it never depends on filesystem path, env, or
# install location, and so re-running the tests from any checkout yields
# the same value. Swapping the sentinel string is a deliberate version
# bump.

_STUB_MODEL_ID = "stub-jepa"
_STUB_MODEL_SENTINEL = b"totali.external.l4l_adapter_stub::StubL4LInferenceAdapter::v1"
_STUB_MODEL_SHA256 = hashlib.sha256(_STUB_MODEL_SENTINEL).hexdigest()

# Embedding-space dimension. The Protocol does not pin this — the real
# JEPA will use 512 or 768. 128 is fine for the stub and keeps logs small.
_STUB_VECTOR_LEN = 128

# Adapter-level cap on ``top_k`` per the Protocol suggestion in
# ``l4l_contract.L4LInferenceAdapter.propose_actions``.
_TOP_K_MAX = 10

# Rotated values for deterministic proposal construction. Order is fixed
# and load-bearing — changing it changes stub output, which is a version
# bump of the stub.
_PROPOSAL_KINDS = (
    "geometry_suggestion",
    "classification_refinement",
    "action_hint",
)
_TARGET_LAYERS: tuple[str | None, ...] = (
    "TOTaLi-SURV-BOUNDARY-DRAFT",
    "TOTaLi-SURV-MONUMENT-DRAFT",
    "TOTaLi-QA-NOTES",
    None,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _canonical_context_bytes(context: dict[str, Any]) -> bytes:
    """Canonicalize a context dict to stable bytes for hashing.

    ``sort_keys=True`` guarantees insertion-order independence. ``default=str``
    guards against non-JSON-serializable values (e.g. ``Path``, ``UUID``)
    without failing — the stub does not care about semantic fidelity of
    the context, only that the same logical context yields the same bytes.
    """

    return json.dumps(context, sort_keys=True, default=str).encode("utf-8")


def _sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _anomaly_score_for_target(target_id: str) -> float:
    """Map a target_id to a deterministic score in ``[0.0, 1.0)``.

    Takes the first 8 hex chars of ``sha256(target_id)`` as a 32-bit
    unsigned int and normalizes by ``2**32`` (i.e. ``(1 << 32)``) — NOT
    ``0xFFFFFFFF``. Using ``2**32`` as the denominator gives a uniform
    distribution over ``2^32`` distinct buckets in the half-open interval
    ``[0.0, 1.0)``; dividing by ``0xFFFFFFFF`` instead would create a
    single over-represented endpoint at exactly 1.0 for one specific
    digest prefix, which is neither uniform nor what callers expect.

    The clamp below is purely defensive — with this denominator no
    arithmetic can exceed 1.0 — but stays in place so future tweaks to
    the formula (e.g., combining multiple hash slices) cannot silently
    overflow the ``L4LAnomalyScore.score`` ``le=1.0`` Pydantic bound.
    """

    digest_hex = hashlib.sha256(target_id.encode("utf-8")).hexdigest()
    raw = int(digest_hex[:8], 16) / (1 << 32)
    return max(0.0, min(1.0, raw))


# ---------------------------------------------------------------------------
# Adapter
# ---------------------------------------------------------------------------


class StubL4LInferenceAdapter:
    """Local stub implementation of L4LInferenceAdapter Protocol.

    Emits deterministic, well-formed, non-authoritative artifacts suitable
    for contract validation and TOTaLi-side integration tests. NOT a real
    JEPA — just enough to satisfy the Protocol and prove callers handle
    the contract correctly.

    When the real L4L-Intelligence ONNX service lands, it will implement
    the same L4LInferenceAdapter Protocol; callers will not need to change.
    """

    def embed_scene(self, context: dict[str, Any]) -> L4LSceneEmbedding:
        """Produce a deterministic ``scene_global`` embedding.

        The context is accepted for Protocol conformance; the stub does
        not vary ``vector_len`` / ``model_id`` with it (those are module
        constants). Callers that need to distinguish two scenes by their
        embedding metadata should switch to the real adapter.
        """

        return L4LSceneEmbedding(
            space="scene_global",
            vector_len=_STUB_VECTOR_LEN,
            model_id=_STUB_MODEL_ID,
            model_sha256=_STUB_MODEL_SHA256,
        )

    def score_anomalies(self, targets: list[str]) -> list[L4LAnomalyScore]:
        """Return one deterministic score per target, preserving order.

        The score is a normalized sha256-prefix, so the mapping
        ``target_id -> score`` is stable for the lifetime of this stub
        version.
        """

        return [
            L4LAnomalyScore(
                target_id=target_id,
                score=_anomaly_score_for_target(target_id),
                model_id=_STUB_MODEL_ID,
                model_sha256=_STUB_MODEL_SHA256,
            )
            for target_id in targets
        ]

    def propose_actions(
        self,
        context: dict[str, Any],
        top_k: int = 5,
    ) -> list[L4LProposal]:
        """Return up to ``top_k`` deterministic proposals.

        Bounds:

        * ``top_k <= 0`` → empty list
        * ``top_k > 10`` → clamped to 10 (Protocol-level suggested cap)

        Proposal properties are derived from a sha256 over the canonical
        context bytes plus a per-slot index so successive proposals in a
        single call differ from each other, and every proposal carries
        ``"stub"`` in its rationale.
        """

        if top_k <= 0:
            return []
        k = min(top_k, _TOP_K_MAX)

        context_digest = _sha256_hex(_canonical_context_bytes(context))

        proposals: list[L4LProposal] = []
        for i in range(k):
            # Mix the slot index into the seed so proposals within a
            # single call are distinct yet still deterministic per
            # (context, slot) pair.
            slot_seed = _sha256_hex(
                f"{context_digest}:{i}".encode("utf-8")
            )
            # Denominator is ``2**32`` for uniform ``[0.0, 1.0)`` — see
            # _anomaly_score_for_target for the rationale on the choice.
            confidence = int(slot_seed[:8], 16) / (1 << 32)
            proposal_kind = _PROPOSAL_KINDS[i % len(_PROPOSAL_KINDS)]
            target_layer = _TARGET_LAYERS[i % len(_TARGET_LAYERS)]

            proposals.append(
                L4LProposal(
                    proposal_kind=proposal_kind,
                    target_layer=target_layer,
                    rationale=(
                        f"{type(self).__name__} stub proposal #{i} "
                        f"(context_digest={context_digest[:12]}...)"
                    ),
                    confidence=max(0.0, min(1.0, confidence)),
                    payload={"slot": i, "seed": slot_seed[:16]},
                )
            )
        return proposals
