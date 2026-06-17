"""U3 (scaffold): board/ALTA-aligned PLS certification record + gate.

Assembles a certification record that references the full defensibility chain
(raw hash -> classified -> extracted -> lint decisions -> certifier identity/seal
+ board/ALTA fields) and enforces DRAFT-until-certified. ``audit/verify.py``
(:func:`verify_certification`) validates completeness and tamper-evidence.

ADVISOR-DEPENDENT (OQ2 / KTD4 / scaffold): the concrete *set* of required board
(design-partner state PLS) + ALTA fields is unknown and MUST be confirmed with
the partner PLS advisor. :data:`REQUIRED_BOARD_ALTA_FIELDS` is intentionally an
empty TODO — do NOT invent field names or values here. The mechanism below
(record structure, chain linkage, the open-item gate, completeness + tamper
verification) is complete and is tested against a caller-supplied required set;
only the production field list awaits the advisor.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Optional, Sequence

from totali.pipeline.models import GeometryStatus, LintItem

# TODO(advisor / OQ2): replace with the design partner's state board PLS rule
# fields + the applicable ALTA/NSPS table-A items, confirmed by the PLS advisor.
# Shipping an empty tuple on purpose — inventing field names would undermine the
# certification's defensibility (the whole moat). `verify_certification` accepts
# a caller-supplied required set so the contract is exercisable before the
# advisor lands.
REQUIRED_BOARD_ALTA_FIELDS: tuple[str, ...] = ()

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
    board/ALTA field set is NOT enforced here (the required set is advisor-defined
    and validated separately by :func:`verify_certification`).
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
