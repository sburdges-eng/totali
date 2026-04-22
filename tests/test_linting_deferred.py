"""L-5: deferred-feature flow.

Surveyor can defer review to next session. Deferred items:
- transition to GeometryStatus.DEFERRED
- stay on the -DRAFT layer (no silent promotion)
- emit a `defer` audit event with reviewer + reason
- block `promote_to_certified` (uncertain ≠ decided)
"""

from totali.audit.logger import AuditLogger
from totali.linting.surveyor_lint import SurveyorLinter
from totali.pipeline.models import GeometryStatus, LintItem


def _audit(tmp_path):
    return AuditLogger(log_dir=str(tmp_path / "audit"), project_id="lint-defer")


class TestDeferItem:
    def test_status_transitions_to_deferred(self, tmp_path):
        audit = _audit(tmp_path)
        item = LintItem(
            item_id="x", geometry_type="LINE",
            layer="TOTaLi-SURV-BRKLN-DRAFT",
        )
        SurveyorLinter.defer_item(item, "PLS Sean", audit, "needs field check")
        assert item.status == GeometryStatus.DEFERRED
        assert item.reviewer == "PLS Sean"
        assert item.notes == "needs field check"
        assert item.review_timestamp is not None

    def test_layer_unchanged_on_defer(self, tmp_path):
        audit = _audit(tmp_path)
        item = LintItem(
            item_id="x", geometry_type="LINE",
            layer="TOTaLi-PLAN-BLDG-DRAFT",
        )
        SurveyorLinter.defer_item(item, "PLS", audit)
        assert item.layer == "TOTaLi-PLAN-BLDG-DRAFT", (
            "deferred items must remain on -DRAFT layer; no silent promotion"
        )

    def test_emits_defer_audit_event(self, tmp_path):
        audit = _audit(tmp_path)
        item = LintItem(
            item_id="abc", geometry_type="POLYGON",
            layer="TOTaLi-PLAN-BLDG-DRAFT",
        )
        SurveyorLinter.defer_item(item, "PLS Jane", audit, "boundary unclear")
        events = audit.get_events("defer")
        assert len(events) == 1
        payload = events[0]["data"]
        assert payload["item_id"] == "abc"
        assert payload["reviewer"] == "PLS Jane"
        assert payload["reason"] == "boundary unclear"
        assert payload["remains_on_layer"] == "TOTaLi-PLAN-BLDG-DRAFT"


class TestPromoteBlocksOnDeferred:
    def test_deferred_items_block_promotion(self, tmp_path):
        audit = _audit(tmp_path)
        items = [
            LintItem(item_id="a", geometry_type="LINE", layer="L-DRAFT",
                     status=GeometryStatus.ACCEPTED),
            LintItem(item_id="b", geometry_type="LINE", layer="L-DRAFT",
                     status=GeometryStatus.DEFERRED),
        ]
        result = SurveyorLinter.promote_to_certified(items, "PLS", "12345", audit)
        assert result is False, "DEFERRED items must block promotion"
        # Accepted item must NOT be silently promoted to CERTIFIED.
        assert items[0].status == GeometryStatus.ACCEPTED

    def test_promote_blocked_audit_mentions_deferred(self, tmp_path):
        audit = _audit(tmp_path)
        items = [
            LintItem(item_id="a", geometry_type="LINE", layer="L-DRAFT",
                     status=GeometryStatus.DEFERRED),
        ]
        SurveyorLinter.promote_to_certified(items, "PLS", "12345", audit)
        events = audit.get_events("promote_blocked")
        assert events
        assert "DEFERRED" in events[0]["data"]["reason"]

    def test_all_accepted_or_rejected_promotes_normally(self, tmp_path):
        audit = _audit(tmp_path)
        items = [
            LintItem(item_id="a", geometry_type="LINE", layer="L-DRAFT",
                     status=GeometryStatus.ACCEPTED),
            LintItem(item_id="b", geometry_type="LINE", layer="L-DRAFT",
                     status=GeometryStatus.REJECTED),
        ]
        result = SurveyorLinter.promote_to_certified(items, "PLS", "12345", audit)
        assert result is True
        assert items[0].status == GeometryStatus.CERTIFIED


class TestDeferredEnumValue:
    def test_deferred_in_geometry_status(self):
        assert GeometryStatus.DEFERRED.value == "DEFERRED"
        # Distinct from every other state.
        all_statuses = {s.value for s in GeometryStatus}
        assert "DEFERRED" in all_statuses
        assert len(all_statuses) >= 6
