"""
Phase 5: Surveyor Linting + Human Certification
=================================================
Ghost suggestions inside CAD → human accepts/rejects → logged.
Outputs remain DRAFT until promoted by certified workflow rules.
AUTO-PROMOTE IS ALWAYS FALSE. PLS remains sovereign.
"""

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from totali.pipeline.models import (
    PhaseResult, GeometryStatus, LintItem, OcclusionType
)
from totali.audit.logger import AuditLogger


class SurveyorLinter:
    def __init__(self, config: dict, audit: AuditLogger):
        self.config = config
        self.audit = audit
        self.ghost_opacity = config.get("ghost_opacity", 0.4)
        self.flag_colors = config.get("flag_colors", {})
        self.auto_promote = False  # HARDCODED FALSE – never auto-promote
        self.require_pls = config.get("require_pls_signature", True)

    def run(self, context: dict) -> PhaseResult:
        manifest = context.get("manifest", {})
        extraction = context.get("extraction")
        classification = context.get("classification")
        output_dir = Path(context.get("output_dir", "output"))

        entities = manifest.get("entities", [])

        # Build lint items from manifest entities
        lint_items = []
        for entity in entities:
            confidence = self._estimate_confidence(entity, classification)
            occlusion = self._check_occlusion(entity, extraction)

            item = LintItem(
                item_id=entity["id"],
                geometry_type=entity["type"],
                layer=entity["layer"],
                status=GeometryStatus.DRAFT,
                confidence=confidence,
                occlusion=occlusion,
                source_hash=entity.get("source_hash", ""),
            )
            lint_items.append(item)

        # Generate lint report
        report = self._generate_lint_report(lint_items, extraction)

        # Write lint report
        report_path = output_dir / "lint_report.json"
        with open(report_path, "w") as f:
            json.dump(report, f, indent=2, default=str)

        # Write review worksheet (human-readable)
        worksheet_path = output_dir / "review_worksheet.txt"
        self._write_review_worksheet(lint_items, extraction, worksheet_path)

        self.audit.log("lint_complete", {
            "total_items": len(lint_items),
            "high_confidence": report["summary"]["high_confidence"],
            "medium_confidence": report["summary"]["medium_confidence"],
            "low_confidence": report["summary"]["low_confidence"],
            "occluded": report["summary"]["occluded"],
            "auto_promote": False,
        })

        return PhaseResult(
            phase="lint",
            success=True,
            message=f"Lint report generated: {len(lint_items)} items for review "
                    f"({report['summary']['low_confidence']} need attention, "
                    f"{report['summary']['occluded']} occluded)",
            data={
                "lint_items": lint_items,
                "lint_report": report,
            },
            output_files=[report_path, worksheet_path],
        )

    def _estimate_confidence(self, entity: dict, classification) -> float:
        """Estimate confidence for a CAD entity based on classification results."""
        if classification is None:
            return 0.5
        # Use mean confidence as proxy (in production, would map entity geometry
        # back to source points and compute per-entity confidence)
        return classification.mean_confidence

    def _check_occlusion(self, entity: dict, extraction) -> OcclusionType:
        """Check if entity overlaps with occlusion zones."""
        if extraction is None or not extraction.occlusion_zones:
            return OcclusionType.NONE

        layer = entity.get("layer", "")
        if "OCCLUSION" in layer:
            return OcclusionType.UNKNOWN

        # Simplified: in production, would do spatial intersection
        return OcclusionType.NONE

    def _generate_lint_report(self, items: list, extraction) -> dict:
        """Generate the full lint report with summary and item details."""
        high = sum(1 for i in items if i.confidence >= 0.75)
        medium = sum(1 for i in items if 0.50 <= i.confidence < 0.75)
        low = sum(1 for i in items if i.confidence < 0.50)
        occluded = sum(1 for i in items if i.occlusion != OcclusionType.NONE)

        report = {
            "generated": datetime.now(timezone.utc).isoformat(),
            "pipeline_version": "0.1.0",
            "auto_promote": False,
            "require_pls_signature": self.require_pls,
            "summary": {
                "total_items": len(items),
                "high_confidence": high,
                "medium_confidence": medium,
                "low_confidence": low,
                "occluded": occluded,
                "status_counts": {
                    GeometryStatus.DRAFT.value: len(items),
                    GeometryStatus.ACCEPTED.value: 0,
                    GeometryStatus.REJECTED.value: 0,
                    GeometryStatus.CERTIFIED.value: 0,
                },
            },
            "qa_flags": extraction.qa_flags if extraction else [],
            "items": [
                {
                    "id": item.item_id,
                    "type": item.geometry_type,
                    "layer": item.layer,
                    "status": item.status.value,
                    "confidence": round(item.confidence, 4),
                    "occlusion": item.occlusion.value,
                    "source_hash": item.source_hash,
                    "color": self._confidence_color(item.confidence),
                }
                for item in items
            ],
            "certification_requirements": {
                "pls_signature_required": self.require_pls,
                "all_items_must_be": "ACCEPTED or REJECTED",
                "no_draft_items_allowed_in_final": True,
                "occlusion_zones_require": "field_verification_plan",
            },
        }

        return report

    def _confidence_color(self, confidence: float) -> str:
        if confidence >= 0.75:
            return self.flag_colors.get("high_confidence", "#00FF00")
        elif confidence >= 0.50:
            return self.flag_colors.get("medium_confidence", "#FFAA00")
        else:
            return self.flag_colors.get("low_confidence", "#FF0000")

    def _write_review_worksheet(
        self, items: list, extraction, path: Path
    ):
        """Write human-readable review worksheet."""
        lines = [
            "=" * 72,
            "TOTaLi DRAFTING – SURVEYOR REVIEW WORKSHEET",
            "=" * 72,
            f"Generated: {datetime.now(timezone.utc).isoformat()}",
            f"Total Items for Review: {len(items)}",
            f"Auto-Promote: DISABLED (PLS certification required)",
            "",
            "-" * 72,
            "QA FLAGS",
            "-" * 72,
        ]

        if extraction and extraction.qa_flags:
            for flag in extraction.qa_flags:
                lines.append(
                    f"  [{flag['severity'].upper()}] {flag['message']}"
                )
        else:
            lines.append("  No QA flags.")

        lines.extend([
            "",
            "-" * 72,
            "ITEMS REQUIRING ATTENTION (Low Confidence / Occluded)",
            "-" * 72,
        ])

        attention_items = [
            i for i in items
            if i.confidence < 0.50 or i.occlusion != OcclusionType.NONE
        ]

        if attention_items:
            for item in attention_items:
                lines.append(
                    f"  ID: {item.item_id}  |  Type: {item.geometry_type}  |  "
                    f"Layer: {item.layer}  |  Conf: {item.confidence:.1%}  |  "
                    f"Occlusion: {item.occlusion.value}"
                )
                lines.append(f"    [ ] ACCEPT   [ ] REJECT   Notes: _______________")
                lines.append("")
        else:
            lines.append("  No items flagged for special attention.")

        lines.extend([
            "",
            "-" * 72,
            "CERTIFICATION",
            "-" * 72,
            "",
            "I have reviewed all AI-assisted drafting output and confirm:",
            "  [ ] All geometry originates from measured data",
            "  [ ] No terrain has been fabricated under occlusion",
            "  [ ] Occlusion zones have field verification plans",
            "  [ ] Layer standards comply with project requirements",
            "",
            "PLS Signature: _________________________  Date: ____________",
            "License No:    _________________________  State: ___________",
            "",
            "=" * 72,
        ])

        with open(path, "w") as f:
            f.write("\n".join(lines))

    # ── Interactive Review API ──────────────────────────────────────────

    @staticmethod
    def accept_item(item: LintItem, reviewer: str, audit: AuditLogger, notes: str = ""):
        """Accept a draft item (called by CAD plugin or review UI)."""
        item.status = GeometryStatus.ACCEPTED
        item.reviewer = reviewer
        item.review_timestamp = datetime.now(timezone.utc).isoformat()
        item.notes = notes
        audit.log("accept", {
            "item_id": item.item_id,
            "reviewer": reviewer,
            "timestamp": item.review_timestamp,
        })

    @staticmethod
    def reject_item(item: LintItem, reviewer: str, audit: AuditLogger, notes: str = ""):
        """Reject a draft item."""
        item.status = GeometryStatus.REJECTED
        item.reviewer = reviewer
        item.review_timestamp = datetime.now(timezone.utc).isoformat()
        item.notes = notes
        audit.log("reject", {
            "item_id": item.item_id,
            "reviewer": reviewer,
            "reason": notes,
            "timestamp": item.review_timestamp,
        })

    @staticmethod
    def promote_to_certified(
        items: list, pls_name: str, pls_license: str, audit: AuditLogger
    ) -> bool:
        """
        Promote all accepted items to certified status.
        Requires ALL items to be either ACCEPTED or REJECTED (no DRAFT remaining).
        Returns False if any items are still in DRAFT status.
        """
        draft_remaining = [i for i in items if i.status == GeometryStatus.DRAFT]
        if draft_remaining:
            audit.log("promote_blocked", {
                "reason": f"{len(draft_remaining)} items still in DRAFT status",
                "pls": pls_name,
            })
            return False

        accepted = [i for i in items if i.status == GeometryStatus.ACCEPTED]
        for item in accepted:
            item.status = GeometryStatus.CERTIFIED
            # Remove -DRAFT suffix from layer name
            if item.layer.endswith("-DRAFT"):
                item.layer = item.layer[:-6]

        audit.log("certify", {
            "pls_name": pls_name,
            "pls_license": pls_license,
            "certified_count": len(accepted),
            "rejected_count": len([i for i in items if i.status == GeometryStatus.REJECTED]),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })

        return True
