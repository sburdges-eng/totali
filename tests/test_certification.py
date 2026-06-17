"""U3 (scaffold): board/ALTA-aligned PLS certification record + gate + verify.

Exercises the certification *mechanism*: a record that references the full
defensibility chain (raw hash -> classified -> extracted -> lint decisions ->
certifier identity/seal + board/ALTA fields), the DRAFT-until-certified gate, and
completeness + tamper verification.

ADVISOR-RESOLVED (OQ2/KTD4): the required board/ALTA field set is confirmed for
the partner jurisdiction (Colorado) — the twelve C.R.S. § 38-51-106(1) plat
elements, the 2021 ALTA/NSPS Section 7 certification, and Table A items 4/5/7/8/11.
These tests assert the production schema plus the generic caller-supplied contract.
"""

from __future__ import annotations

import pytest

from totali.audit.verify import verify_certification
from totali.linting.certification import (
    CERT_SCHEMA,
    REQUIRED_BOARD_ALTA_FIELDS,
    CertField,
    CertificationBlocked,
    CertifierIdentity,
    CertificationRecord,
    certify,
    export_blocked,
    missing_required_fields,
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


class TestColoradoAltaSchema:
    def test_required_set_is_populated_and_consistent_with_schema(self):
        assert REQUIRED_BOARD_ALTA_FIELDS  # no longer an empty TODO
        assert REQUIRED_BOARD_ALTA_FIELDS == tuple(f.key for f in CERT_SCHEMA)
        assert len(REQUIRED_BOARD_ALTA_FIELDS) == len(set(REQUIRED_BOARD_ALTA_FIELDS))

    def test_twelve_colorado_plat_elements_present(self):
        co = [f for f in CERT_SCHEMA if f.category == "co_plat"]
        assert len(co) == 12
        assert all("38-51-106" in f.authority for f in co)

    def test_alta_section7_and_table_a_items_present(self):
        s7 = [f for f in CERT_SCHEMA if f.category == "alta_section7"]
        table_a = [f for f in CERT_SCHEMA if f.category == "alta_table_a"]
        assert s7 and all("§ 7" in f.authority for f in s7)
        # LiDAR/drone scope = Table A items 4, 5, 7, 8, 11.
        items = {f.key for f in table_a}
        assert items == {
            "alta_table_a_4_gross_land_area",
            "alta_table_a_5_vertical_relief",
            "alta_table_a_7_building_dimensions",
            "alta_table_a_8_substantial_features",
            "alta_table_a_11_underground_utility_evidence",
        }
        assert all("Table A" in f.authority for f in table_a)

    def test_every_field_has_authority_label_and_category(self):
        for f in CERT_SCHEMA:
            assert isinstance(f, CertField)
            assert f.key and f.authority and f.label
            assert f.category in {"co_plat", "alta_section7", "alta_table_a"}


def _full_board_fields():
    """A board/ALTA payload populating every required key (non-empty)."""
    return {k: f"value::{k}" for k in REQUIRED_BOARD_ALTA_FIELDS}


class TestProductionSchemaCompleteness:
    def test_full_colorado_record_verifies_against_production_set(self):
        record = _certify(
            [_item("e1", GeometryStatus.ACCEPTED)],
            board_alta_fields=_full_board_fields(),
        )
        ok, errors = verify_certification(
            record, required_fields=REQUIRED_BOARD_ALTA_FIELDS
        )
        assert ok is True, errors
        assert missing_required_fields(record) == []

    def test_missing_colorado_element_fails(self):
        fields = _full_board_fields()
        del fields["co_plat_basis_of_bearings"]
        record = _certify(
            [_item("e1", GeometryStatus.ACCEPTED)], board_alta_fields=fields
        )
        ok, errors = verify_certification(
            record, required_fields=REQUIRED_BOARD_ALTA_FIELDS
        )
        assert ok is False
        assert any("co_plat_basis_of_bearings" in e for e in errors)
        assert "co_plat_basis_of_bearings" in missing_required_fields(record)

    def test_empty_value_counts_as_missing(self):
        fields = _full_board_fields()
        fields["alta_table_a_5_vertical_relief"] = ""  # present but empty
        record = _certify(
            [_item("e1", GeometryStatus.ACCEPTED)], board_alta_fields=fields
        )
        ok, errors = verify_certification(
            record, required_fields=REQUIRED_BOARD_ALTA_FIELDS
        )
        assert ok is False
        assert any("alta_table_a_5_vertical_relief" in e for e in errors)


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
