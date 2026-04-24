"""Phase 3A: end-to-end composition of the auracad + L4L adapters.

This test proves the two Phase-1/Phase-2 adapters compose across realistic
pipeline-stage boundaries without adapter-side massaging:

* :class:`totali.external.auracad_adapter_dxf.DxfAuracadAdapter`
  (Phase 1) parses a DXF into an :class:`AuracadReadReport`.
* :class:`totali.external.l4l_adapter_stub.StubL4LInferenceAdapter`
  (Phase 2) consumes the report's entity IDs / layer names / file
  metadata as inputs to ``score_anomalies`` / ``propose_actions`` /
  ``embed_scene``.

The assertions focus on:

* typing: the ``id`` field produced on the auracad side is the ``str``
  the L4L side's ``list[str]`` signature expects (no inter-adapter
  translation layer required),
* invariant: every L4L output in the chain is non-authoritative
  (§1.3 TOTaLi invariant),
* determinism: replaying the entire two-adapter chain on the same
  fixture yields identical proposal rationales,
* failure modes: missing DXF / empty target list / ``top_k=0`` each
  surface at the adapter boundary they originate from; none propagate
  silently.

Production code is unchanged; this is a pure test-level integration
check.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

# ``DxfAuracadAdapter`` routes through ``dwg-tool-parser/scripts/parse_dxf.py``
# which imports ezdxf. Skip the whole module when ezdxf isn't installed.
pytest.importorskip("ezdxf")

from totali.external.auracad_adapter_dxf import DxfAuracadAdapter
from totali.external.auracad_contract import AuracadReadReport
from totali.external.l4l_adapter_stub import StubL4LInferenceAdapter
from totali.external.l4l_contract import (
    L4LAnomalyScore,
    L4LProposal,
    L4LSceneEmbedding,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
FIXTURE_DXF = REPO_ROOT / "dwg-tool-parser" / "fixtures" / "sample.dxf"


@pytest.fixture(scope="module")
def sample_dxf_path() -> str:
    assert FIXTURE_DXF.exists(), (
        f"committed DXF fixture missing at {FIXTURE_DXF}; "
        "regenerate via tests/test_dwg_parser_ezdxf.py first"
    )
    return str(FIXTURE_DXF)


@pytest.fixture
def cad_adapter() -> DxfAuracadAdapter:
    return DxfAuracadAdapter()


@pytest.fixture
def l4l_adapter() -> StubL4LInferenceAdapter:
    return StubL4LInferenceAdapter()


@pytest.fixture
def cad_report(
    cad_adapter: DxfAuracadAdapter, sample_dxf_path: str
) -> AuracadReadReport:
    return cad_adapter.read_cad(sample_dxf_path)


def _context_from_report(report: AuracadReadReport) -> dict[str, Any]:
    """Build the realistic L4L-context dict from an AuracadReadReport.

    This mirrors the dict a real pipeline stage would construct — only
    fields directly attested by the auracad side. No adapter-side
    massaging; the keys are the auracad report's own field names.
    """

    return {
        "file": report.file,
        "format": report.format,
        "entity_count": len(report.entities),
        "layers": [lyr["name"] for lyr in report.layers],
    }


# ===================================================================
# 1. CAD -> L4L.score_anomalies
# ===================================================================


class TestCADToL4LPipeline:
    """Flow: ``read_cad`` entity IDs -> ``score_anomalies`` targets."""

    def test_read_cad_produces_entities(
        self, cad_report: AuracadReadReport
    ) -> None:
        # Guardrail: the whole test class is meaningless if the fixture
        # suddenly reports zero entities.
        assert len(cad_report.entities) >= 1

    def test_entity_id_field_is_str(
        self, cad_report: AuracadReadReport
    ) -> None:
        """Output-to-input typing proof.

        The auracad-side ``entities[i]["id"]`` must be ``str`` so it
        drops straight into ``score_anomalies(targets: list[str])``
        without conversion. If parse_dxf ever starts emitting ints or
        UUIDs, this test fails loudly.
        """

        for entity in cad_report.entities:
            assert "id" in entity, f"entity missing 'id' field: {entity}"
            assert isinstance(entity["id"], str), (
                f"entity id must be str for direct composition into "
                f"L4L.score_anomalies; got {type(entity['id']).__name__}"
            )

    def test_entity_ids_flow_into_score_anomalies(
        self,
        cad_report: AuracadReadReport,
        l4l_adapter: StubL4LInferenceAdapter,
    ) -> None:
        entity_ids = [entity["id"] for entity in cad_report.entities]
        scores = l4l_adapter.score_anomalies(entity_ids)

        # One score per entity, preserving order.
        assert len(scores) == len(entity_ids)
        for score, entity_id in zip(scores, entity_ids, strict=True):
            assert isinstance(score, L4LAnomalyScore)
            assert score.target_id == entity_id

    def test_all_scores_non_authoritative(
        self,
        cad_report: AuracadReadReport,
        l4l_adapter: StubL4LInferenceAdapter,
    ) -> None:
        entity_ids = [entity["id"] for entity in cad_report.entities]
        scores = l4l_adapter.score_anomalies(entity_ids)
        for score in scores:
            assert score.authoritative is False

    def test_all_scores_in_half_open_unit_interval(
        self,
        cad_report: AuracadReadReport,
        l4l_adapter: StubL4LInferenceAdapter,
    ) -> None:
        """Scores must fall in ``[0, 1)``.

        The Pydantic field bound is ``[0.0, 1.0]`` (inclusive top), but
        the stub's hash-prefix math divides by ``2**32`` and therefore
        cannot reach exactly 1.0 — pinning the tighter half-open bound
        here keeps the composition honest about what the stub actually
        produces.
        """

        entity_ids = [entity["id"] for entity in cad_report.entities]
        scores = l4l_adapter.score_anomalies(entity_ids)
        for score in scores:
            assert 0.0 <= score.score < 1.0


# ===================================================================
# 2. CAD -> L4L.propose_actions (with context built from report fields)
# ===================================================================


class TestCADToL4LProposals:
    """Flow: report fields -> context dict -> ``propose_actions``."""

    def test_proposals_returned_for_cad_context(
        self,
        cad_report: AuracadReadReport,
        l4l_adapter: StubL4LInferenceAdapter,
    ) -> None:
        context = _context_from_report(cad_report)
        proposals = l4l_adapter.propose_actions(context, top_k=3)

        assert len(proposals) == 3
        for proposal in proposals:
            assert isinstance(proposal, L4LProposal)

    def test_all_proposals_non_authoritative(
        self,
        cad_report: AuracadReadReport,
        l4l_adapter: StubL4LInferenceAdapter,
    ) -> None:
        context = _context_from_report(cad_report)
        proposals = l4l_adapter.propose_actions(context, top_k=3)
        for proposal in proposals:
            assert proposal.authoritative is False

    def test_chain_is_deterministic_end_to_end(
        self, sample_dxf_path: str
    ) -> None:
        """Replay the full two-adapter chain twice; rationales identical.

        Runs two fresh adapter pairs so determinism holds across adapter
        instantiations, not just across calls on one adapter. This is
        the stability proof the pipeline relies on — a DRAFT proposal
        replayed tomorrow must match the one emitted today for
        provenance audits.
        """

        cad_a = DxfAuracadAdapter()
        l4l_a = StubL4LInferenceAdapter()
        report_a = cad_a.read_cad(sample_dxf_path)
        proposals_a = l4l_a.propose_actions(
            _context_from_report(report_a), top_k=3
        )

        cad_b = DxfAuracadAdapter()
        l4l_b = StubL4LInferenceAdapter()
        report_b = cad_b.read_cad(sample_dxf_path)
        proposals_b = l4l_b.propose_actions(
            _context_from_report(report_b), top_k=3
        )

        rationales_a = [p.rationale for p in proposals_a]
        rationales_b = [p.rationale for p in proposals_b]
        assert rationales_a == rationales_b


# ===================================================================
# 3. CAD -> L4L.embed_scene
# ===================================================================


class TestEmbeddingFromCADContext:
    """Flow: report.file -> ``embed_scene`` context."""

    def test_embed_scene_returns_valid_embedding(
        self,
        cad_report: AuracadReadReport,
        l4l_adapter: StubL4LInferenceAdapter,
    ) -> None:
        emb = l4l_adapter.embed_scene({"report_file": cad_report.file})
        assert isinstance(emb, L4LSceneEmbedding)
        assert emb.authoritative is False
        assert emb.space == "scene_global"

    def test_model_sha256_stable_across_contexts(
        self,
        cad_report: AuracadReadReport,
        l4l_adapter: StubL4LInferenceAdapter,
    ) -> None:
        """``model_sha256`` must equal the stub's module-level constant
        regardless of whether ``embed_scene`` was called inside or
        outside the pipeline. The stub advertises a fixed model
        identity; a pipeline wrapper that mutated it would break
        provenance.
        """

        in_pipeline = l4l_adapter.embed_scene(
            {"report_file": cad_report.file}
        )
        out_of_pipeline = l4l_adapter.embed_scene({"tile_id": "standalone"})

        assert in_pipeline.model_sha256 == out_of_pipeline.model_sha256
        assert in_pipeline.model_id == out_of_pipeline.model_id


# ===================================================================
# 4. Failure modes surface at their originating adapter
# ===================================================================


class TestFailureModesCompose:
    """Degenerate inputs must not propagate silently through the chain."""

    def test_missing_dxf_raises_oserror_on_cad_side(
        self, cad_adapter: DxfAuracadAdapter
    ) -> None:
        # parse_dxf raises FileNotFoundError (an OSError subclass) for
        # missing files; the adapter does not swallow it.
        with pytest.raises(OSError):
            cad_adapter.read_cad("/nonexistent/path/definitely_not_a_dxf.dxf")

    def test_empty_targets_returns_empty_scores(
        self, l4l_adapter: StubL4LInferenceAdapter
    ) -> None:
        # Mirrors the L4L-side empty-list contract, exercised here as
        # the degenerate "auracad produced zero entities" case.
        assert l4l_adapter.score_anomalies([]) == []

    def test_top_k_zero_returns_empty_proposals(
        self,
        cad_report: AuracadReadReport,
        l4l_adapter: StubL4LInferenceAdapter,
    ) -> None:
        context = _context_from_report(cad_report)
        assert l4l_adapter.propose_actions(context, top_k=0) == []


# ===================================================================
# 5. Non-authoritative invariant across the chain
# ===================================================================


class TestNoAuthoritativeOutputInChain:
    """End-to-end §1.3 invariant: no L4L output anywhere in the chain
    can be authoritative, regardless of which auracad-side data fed it.

    ``AuracadReadReport`` has no ``authoritative`` field by design —
    auracad IS authoritative for the geometry it returns, per the
    contract docstring. The L4L side then attaches a non-authoritative
    advisory layer on top. This test pins that boundary.
    """

    def test_l4l_outputs_from_cad_chain_are_all_non_authoritative(
        self,
        cad_report: AuracadReadReport,
        l4l_adapter: StubL4LInferenceAdapter,
    ) -> None:
        entity_ids = [entity["id"] for entity in cad_report.entities]
        context = _context_from_report(cad_report)

        emb = l4l_adapter.embed_scene({"report_file": cad_report.file})
        scores = l4l_adapter.score_anomalies(entity_ids)
        proposals = l4l_adapter.propose_actions(context, top_k=5)

        assert emb.authoritative is False
        for score in scores:
            assert score.authoritative is False
        for proposal in proposals:
            assert proposal.authoritative is False

    def test_auracad_report_has_no_authoritative_field(
        self, cad_report: AuracadReadReport
    ) -> None:
        """Pins the contract asymmetry: only L4L outputs carry the
        ``authoritative`` flag. If auracad ever grows one, the
        invariant-composition story here needs rethinking.
        """

        assert "authoritative" not in type(cad_report).model_fields
