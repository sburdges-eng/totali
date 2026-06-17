"""U3 (scaffold): board/ALTA-aligned PLS certification record + gate + verify.

Exercises the certification *mechanism*: a record that references the full
defensibility chain (raw hash -> classified -> extracted -> lint decisions ->
certifier identity/seal + board/ALTA fields), the DRAFT-until-certified gate, and
completeness + tamper verification.

ADVISOR-DEPENDENT (OQ2/KTD4): the concrete required board/ALTA field *set* is
unknown and must be confirmed with the design-partner PLS advisor. The shipped
default (`REQUIRED_BOARD_ALTA_FIELDS`) is an empty TODO; these tests supply a
generic placeholder required set to exercise the contract without inventing real
board/ALTA field names.
"""

from __future__ import annotations

import pytest

from totali.audit.verify import verify_certification
from totali.linting.certification import (
    REQUIRED_BOARD_ALTA_FIELDS,
    CertificationBlocked,
    CertifierIdentity,
    CertificationRecord,
    certify,
    export_blocked,
)
from totali.pipeline.models import GeometryStatus, LintItem


def _item(item_id, status):
    return LintItem(item_id=item_id, geometry_type="breakline", layer="TOTaLi-SURV-BRKLN-DRAFT", status=status)


def _certifier():
    return CertifierIdentity(
        name="Jane Roe", license_number="PLS-0000", jurisdiction="Example State Board",
        seal_ref="seal://example",
    )


# Generic placeholder required fields — NOT real board/ALTA names (advisor TODO).
_REQ = ("required_field_x", "required_field_y")
_FIELDS = {"required_field_x": "value-x", "required_field_y": "value-y"}


def _certify(lint_items, board_alta_fields=None, defer_reason=None, audit=None):
    return certify(
        project_id="proj-1",
        certifier=_certifier(),
        raw_hash="a" * 64,
        classified_ref="classify-ref",
        extracted_ref="extract-ref",
        lint_items=lint_items,
        board_alta_fields=board_alta_fields if board_alta_fields is not None else dict(_FIELDS),
        defer_reason=defer_reason,
        audit=audit,
    )


class TestAdvisorTodo:
    def test_required_fields_default_is_empty_placeholder(self):
        # Hard constraint: do not ship invented board/ALTA field names.
        assert REQUIRED_BOARD_ALTA_FIELDS == ()


class TestDraftGate:
    def test_open_items_block_certification(self):
        items = [_item("e1", GeometryStatus.ACCEPTED), _item("e2", GeometryStatus.DRAFT)]
        with pytest.raises(CertificationBlocked):
            _certify(items)

    def test_open_items_allowed_when_deferred_with_reason(self):
        items = [_item("e1", GeometryStatus.ACCEPTED), _item("e2", GeometryStatus.DRAFT)]
        record = _certify(items, defer_reason="field verification scheduled 2026-07-01")
        assert record.status == GeometryStatus.CERTIFIED.value
        assert record.defer_reason

    def test_all_resolved_certifies(self):
        items = [_item("e1", GeometryStatus.ACCEPTED), _item("e2", GeometryStatus.REJECTED)]
        record = _certify(items)
        assert isinstance(record, CertificationRecord)
        assert record.status == GeometryStatus.CERTIFIED.value
        # References the full chain.
        assert record.raw_hash == "a" * 64
        assert record.classified_ref == "classify-ref"
        assert record.extracted_ref == "extract-ref"
        assert record.lint_decision_refs == ["e1:ACCEPTED", "e2:REJECTED"]


class TestRequiredFieldsCompleteness:
    def test_record_with_all_required_fields_verifies(self):
        record = _certify([_item("e1", GeometryStatus.ACCEPTED)])
        ok, errors = verify_certification(record, required_fields=_REQ)
        assert ok is True, errors

    def test_missing_required_field_fails_verification(self):
        record = _certify(
            [_item("e1", GeometryStatus.ACCEPTED)],
            board_alta_fields={"required_field_x": "value-x"},  # missing _y
        )
        ok, errors = verify_certification(record, required_fields=_REQ)
        assert ok is False
        assert any("required_field_y" in e for e in errors)


class TestChainAndTamperVerification:
    def test_incomplete_chain_detected(self):
        record = _certify([_item("e1", GeometryStatus.ACCEPTED)])
        record.extracted_ref = ""  # break a chain link
        ok, errors = verify_certification(record, required_fields=_REQ)
        assert ok is False
        assert any("extracted_ref" in e for e in errors)

    def test_tampered_record_hash_detected(self):
        record = _certify([_item("e1", GeometryStatus.ACCEPTED)])
        good = record.record_hash()
        record.board_alta_fields["required_field_x"] = "TAMPERED"
        ok, errors = verify_certification(record, required_fields=_REQ, expected_record_hash=good)
        assert ok is False
        assert any("tamper" in e.lower() or "hash" in e.lower() for e in errors)

    def test_incomplete_certifier_detected(self):
        record = _certify([_item("e1", GeometryStatus.ACCEPTED)])
        record.certifier.license_number = ""
        ok, errors = verify_certification(record, required_fields=_REQ)
        assert ok is False
        assert any("license" in e.lower() or "certifier" in e.lower() for e in errors)


class TestExportGate:
    def test_draft_blocks_export(self):
        assert export_blocked([_item("e1", GeometryStatus.DRAFT)]) is True

    def test_all_resolved_does_not_block_export(self):
        items = [_item("e1", GeometryStatus.ACCEPTED), _item("e2", GeometryStatus.REJECTED)]
        assert export_blocked(items) is False


class TestAuditEmission:
    def test_certify_emits_audit_event(self, audit_logger):
        _certify([_item("e1", GeometryStatus.ACCEPTED)], audit=audit_logger)
        events = audit_logger.get_events("certify")
        assert len(events) == 1
        assert events[0]["data"]["pls_license"] == "PLS-0000"
