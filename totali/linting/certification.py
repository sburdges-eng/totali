"""U3 (scaffold): board/ALTA-aligned PLS certification record + gate.

Assembles a certification record that references the full defensibility chain
(raw hash -> classified -> extracted -> lint decisions -> certifier identity/seal
+ board/ALTA fields) and enforces DRAFT-until-certified. ``audit/verify.py``
(:func:`verify_certification`) validates completeness and tamper-evidence.

ADVISOR-RESOLVED (OQ2 / KTD4): the required board/ALTA field set has been
confirmed for the design partner's jurisdiction (Colorado). It enforces the
twelve statutory land-survey-plat elements of C.R.S. § 38-51-106(1) alongside the
2021 ALTA/NSPS Section 7 certification and the Table A items in scope for the
LiDAR/drone classifier pipeline (items 4, 5, 7, 8, 11). See :data:`CERT_SCHEMA`
for the per-field authority citations and :data:`REQUIRED_BOARD_ALTA_FIELDS` for
the enforced key set; :func:`totali.audit.verify.verify_certification` validates
completeness against it. Field *values* remain the surveyor's to supply per job;
this schema fixes which elements a certified Colorado deliverable must carry.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Optional, Sequence

from totali.pipeline.models import GeometryStatus, LintItem

@dataclass(frozen=True)
class CertField:
    """One required certification element, with its governing authority.

    ``key`` is the stable field name carried in
    :attr:`CertificationRecord.board_alta_fields`; ``authority`` is the statutory
    or standard citation that makes the element mandatory (the defensibility
    anchor); ``category`` groups by source.
    """

    key: str
    authority: str
    label: str
    category: str  # "co_plat" | "alta_section7" | "alta_table_a"


# Colorado statutory land-survey-plat elements — C.R.S. § 38-51-106(1)(a)-(l).
# Subsection letters follow the (1)(a)-(l) enumeration; the elements (not the
# letters) are the binding content and are advisor-confirmed for the partner.
_CO_PLAT_FIELDS: tuple[CertField, ...] = (
    CertField("co_plat_boundary_scale_drawing", "C.R.S. § 38-51-106(1)(a)",
              "Scale drawing of the boundaries of the land parcel", "co_plat"),
    CertField("co_plat_rights_of_way_easements", "C.R.S. § 38-51-106(1)(b)",
              "Recorded and apparent rights-of-way and easements (with source), "
              "or the client's statement electing not to show them", "co_plat"),
    CertField("co_plat_field_measured_dimensions", "C.R.S. § 38-51-106(1)(c)",
              "Field-measured dimensions necessary to establish the boundaries "
              "on the ground", "co_plat"),
    CertField("co_plat_responsible_charge_statement", "C.R.S. § 38-51-106(1)(d)",
              "Statement that the survey was performed by, or under the "
              "responsible charge of, the professional land surveyor", "co_plat"),
    CertField("co_plat_basis_of_bearings", "C.R.S. § 38-51-106(1)(e)",
              "Statement explaining how bearings were determined", "co_plat"),
    CertField("co_plat_monuments_found_and_set", "C.R.S. § 38-51-106(1)(f)",
              "Description of all monuments found and set and all control "
              "monuments used in the survey", "co_plat"),
    CertField("co_plat_scale_and_bar", "C.R.S. § 38-51-106(1)(g)",
              "Statement of scale or representative fraction and a bar/graphical "
              "scale", "co_plat"),
    CertField("co_plat_north_arrow", "C.R.S. § 38-51-106(1)(h)",
              "North arrow", "co_plat"),
    CertField("co_plat_property_description", "C.R.S. § 38-51-106(1)(i)",
              "Written property description (county, state, section, township, "
              "range, principal meridian or established subdivision/block/lot)",
              "co_plat"),
    CertField("co_plat_signature_and_seal", "C.R.S. § 38-51-106(1)(j)",
              "Signature and seal of the professional land surveyor", "co_plat"),
    CertField("co_plat_conflicting_boundary_evidence", "C.R.S. § 38-51-106(1)(k)",
              "Any conflicting boundary evidence", "co_plat"),
    CertField("co_plat_linear_units_statement", "C.R.S. § 38-51-106(1)(l)",
              "Statement defining the linear units used (conversion derived from "
              "the meter as defined by NIST)", "co_plat"),
)

# 2021 ALTA/NSPS Section 7 certification statement components.
_ALTA_SECTION7_FIELDS: tuple[CertField, ...] = (
    CertField("alta_s7_certified_to", "2021 ALTA/NSPS § 7",
              "Parties to whom the survey is certified", "alta_section7"),
    CertField("alta_s7_fieldwork_completion_date", "2021 ALTA/NSPS § 7",
              "Date of the fieldwork completion stated in the certification",
              "alta_section7"),
    CertField("alta_s7_standard_reference", "2021 ALTA/NSPS § 7",
              "Certification references the 2021 Minimum Standard Detail "
              "Requirements for ALTA/NSPS Land Title Surveys and the Table A "
              "items selected", "alta_section7"),
)

# 2021 ALTA/NSPS Table A optional items in scope for the LiDAR/drone pipeline.
_ALTA_TABLE_A_FIELDS: tuple[CertField, ...] = (
    CertField("alta_table_a_4_gross_land_area", "2021 ALTA/NSPS Table A, Item 4",
              "Gross land area (and other areas if specified by the client)",
              "alta_table_a"),
    CertField("alta_table_a_5_vertical_relief", "2021 ALTA/NSPS Table A, Item 5",
              "Vertical relief with source of information, contour interval, "
              "datum, and originating benchmark", "alta_table_a"),
    CertField("alta_table_a_7_building_dimensions", "2021 ALTA/NSPS Table A, Item 7",
              "Exterior building dimensions at ground level, exterior-footprint "
              "square footage, and measured building height (7a-c)", "alta_table_a"),
    CertField("alta_table_a_8_substantial_features", "2021 ALTA/NSPS Table A, Item 8",
              "Substantial features observed during fieldwork (parking, signs, "
              "pools, landscaped areas, refuse, etc.)", "alta_table_a"),
    CertField("alta_table_a_11_underground_utility_evidence",
              "2021 ALTA/NSPS Table A, Item 11",
              "Evidence of underground utilities (11a plans/reports; 11b private "
              "locate markings) with the mandatory client/insurer/lender note",
              "alta_table_a"),
)

# Advisor-resolved required certification schema for the partner jurisdiction
# (Colorado). Ordered: statutory plat elements, then ALTA Section 7, then Table A.
CERT_SCHEMA: tuple[CertField, ...] = (
    *_CO_PLAT_FIELDS,
    *_ALTA_SECTION7_FIELDS,
    *_ALTA_TABLE_A_FIELDS,
)

# The enforced key set, derived from CERT_SCHEMA (single source of truth).
REQUIRED_BOARD_ALTA_FIELDS: tuple[str, ...] = tuple(f.key for f in CERT_SCHEMA)

# Empty-equivalents that do not satisfy a required field.
_EMPTY_VALUES = (None, "", [], {}, ())


def missing_required_fields(
    record: "CertificationRecord",
    required: Sequence[str] = REQUIRED_BOARD_ALTA_FIELDS,
) -> list[str]:
    """Required field keys absent or empty in ``record.board_alta_fields``."""
    present = record.board_alta_fields or {}
    return [
        k for k in required
        if k not in present or present[k] in _EMPTY_VALUES
    ]

# Lint statuses that still need a surveyor decision before certification.
_OPEN_STATUSES = frozenset({GeometryStatus.DRAFT, GeometryStatus.FLAGGED})


class CertificationBlocked(Exception):
    """Raised when open lint items remain and no deferral reason was given."""


@dataclass
class CertifierIdentity:
    name: str
    license_number: str
    jurisdiction: str
    seal_ref: Optional[str] = None

    def is_complete(self) -> bool:
        return bool(self.name and self.license_number and self.jurisdiction)


@dataclass
class CertificationRecord:
    project_id: str
    certifier: CertifierIdentity
    raw_hash: str
    classified_ref: str
    extracted_ref: str
    lint_decision_refs: list[str]
    board_alta_fields: dict[str, Any] = field(default_factory=dict)
    defer_reason: Optional[str] = None
    status: str = GeometryStatus.CERTIFIED.value
    timestamp: str = ""

    def chain_refs(self) -> list[str]:
        """Ordered references along the defensibility chain."""
        return [self.raw_hash, self.classified_ref, self.extracted_ref, *self.lint_decision_refs]

    def record_hash(self) -> str:
        """SHA-256 over the canonical record content (tamper-evidence)."""
        payload = {
            "project_id": self.project_id,
            "certifier": asdict(self.certifier),
            "raw_hash": self.raw_hash,
            "classified_ref": self.classified_ref,
            "extracted_ref": self.extracted_ref,
            "lint_decision_refs": self.lint_decision_refs,
            "board_alta_fields": self.board_alta_fields,
            "defer_reason": self.defer_reason,
            "status": self.status,
        }
        blob = json.dumps(payload, sort_keys=True, default=str).encode("utf-8")
        return hashlib.sha256(blob).hexdigest()

    def to_dict(self) -> dict:
        out = asdict(self)
        out["record_hash"] = self.record_hash()
        return out


def export_blocked(lint_items: Sequence[LintItem]) -> bool:
    """DRAFT-until-certified: True while any item still needs a decision.

    Mirrors the promote gate in ``surveyor_lint.py`` — DRAFT/FLAGGED output must
    not be exported as a deliverable until the surveyor resolves (or explicitly
    defers) it.
    """
    return any(i.status in _OPEN_STATUSES for i in lint_items)


def certify(
    *,
    project_id: str,
    certifier: CertifierIdentity,
    raw_hash: str,
    classified_ref: str,
    extracted_ref: str,
    lint_items: Sequence[LintItem],
    board_alta_fields: dict[str, Any],
    defer_reason: Optional[str] = None,
    audit: Any = None,
    timestamp: Optional[str] = None,
) -> CertificationRecord:
    """Build a certification record after enforcing the open-item gate.

    Raises :class:`CertificationBlocked` if any lint item is still open
    (DRAFT/FLAGGED) and no ``defer_reason`` is supplied. Completeness of the
    board/ALTA field set (:data:`REQUIRED_BOARD_ALTA_FIELDS`) is NOT enforced
    here; it is validated separately by :func:`verify_certification` /
    :func:`missing_required_fields`, so a record can be assembled incrementally.
    """
    open_items = [i for i in lint_items if i.status in _OPEN_STATUSES]
    if open_items and not defer_reason:
        raise CertificationBlocked(
            f"{len(open_items)} lint item(s) still open "
            f"({[i.item_id for i in open_items]}); resolve them or pass an "
            "explicit defer_reason. Certification cannot proceed over silent DRAFTs."
        )

    record = CertificationRecord(
        project_id=project_id,
        certifier=certifier,
        raw_hash=raw_hash,
        classified_ref=classified_ref,
        extracted_ref=extracted_ref,
        lint_decision_refs=[f"{i.item_id}:{i.status.value}" for i in lint_items],
        board_alta_fields=dict(board_alta_fields),
        defer_reason=defer_reason,
        status=GeometryStatus.CERTIFIED.value,
        timestamp=timestamp or datetime.now(timezone.utc).isoformat(),
    )

    if audit is not None:
        audit.log("certify", {
            "project_id": project_id,
            "pls_name": certifier.name,
            "pls_license": certifier.license_number,
            "jurisdiction": certifier.jurisdiction,
            "raw_hash": raw_hash,
            "chain_refs": record.chain_refs(),
            "board_alta_field_keys": sorted(record.board_alta_fields),
            "record_hash": record.record_hash(),
            "defer_reason": defer_reason,
        })

    return record
